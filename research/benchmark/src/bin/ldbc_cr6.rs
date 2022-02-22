#[macro_use]
extern crate lazy_static;

use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::prelude::*;
use pegasus::api::{CorrelatedSubTask, Count, Dedup, Filter, HasAny, Map, Merge, Sink};
use pegasus::configure_with_default;
use pegasus::stream::Stream;
use pegasus::{Configuration, JobConf};
use std::path::Path;
use structopt::StructOpt;

lazy_static! {
    pub static ref GRAPH: LargeGraphDB<DefaultId, InternalId> = _init_graph();
    pub static ref DATA_PATH: String = configure_with_default!(String, "DATA_PATH", "".to_string());
    pub static ref PARTITION_ID: usize = configure_with_default!(usize, "PARTITION_ID", 0);
}

fn _init_graph() -> LargeGraphDB<DefaultId, InternalId> {
    println!("Read the graph data from {:?} for demo.", *DATA_PATH);
    GraphDBConfig::default()
        .root_dir(&(*DATA_PATH))
        .partition(*PARTITION_ID)
        .schema_file(
            &(DATA_PATH.as_ref() as &Path)
                .join(DIR_GRAPH_SCHEMA)
                .join(FILE_SCHEMA),
        )
        .open()
        .expect("Open graph error")
}

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "s", long = "starter", default_value = "72088380363511554")]
    starter: u64,
    #[structopt(short = "a", long = "is_apply")]
    is_use_apply: bool,
    #[structopt(short = "w", long = "workers", default_value = "2")]
    workers: u32,
}

fn main() {
    let config: Config = Config::from_args();
    pegasus_common::logs::init_log();
    // May switch to distributed
    pegasus::startup(Configuration::singleton()).ok();

    let mut conf = JobConf::new("word count");
    conf.set_workers(config.workers);
    let starter = config.starter;
    let is_use_apply = config.is_use_apply;

    let mut result = pegasus::run(conf, || {
        let id = pegasus::get_current_worker().index;
        move |input, output| {
            let start = if id == 0 {
                vec![starter].into_iter()
            } else {
                vec![].into_iter()
            };
            let (stream1, stream2) = input
                .input_from(start)?
                .repartition(|id| Ok(*id))
                .flat_map(|id| {
                    Ok(GRAPH
                        .get_both_vertices(id as DefaultId, Some(&vec![12]))
                        .map(|v| v.get_id() as u64))
                })?
                .copied()?;

            let mut stream = stream1
                .merge(stream2.repartition(|id| Ok(*id)).flat_map(|id| {
                    Ok(GRAPH
                        .get_both_vertices(id as DefaultId, Some(&vec![12]))
                        .map(|v| v.get_id() as u64))
                })?)?
                .dedup()?
                .filter(move |id| Ok(*id != starter))?
                .repartition(|id| Ok(*id))
                .flat_map(|id| {
                    Ok(GRAPH
                        .get_in_vertices(id as DefaultId, Some(&vec![0]))
                        .map(|v| (v.get_id() as u64, v.get_label()[0])))
                })?
                .filter(|(_, label)| Ok(*label == 3))?
                .map(|(v, _)| Ok(v))?;

            let subtask = |input: Stream<u64>| {
                input
                    .repartition(|id| Ok(*id))
                    .flat_map(|id| {
                        Ok(GRAPH
                            .get_out_vertices(id as DefaultId, Some(&vec![1]))
                            .map(|v| v.get_id() as u64))
                    })
                    .expect("build subtask error!")
                    .repartition(|v| Ok(*v))
                    .filter(|v| {
                        if let Some(lv) = GRAPH.get_vertex(*v as DefaultId) {
                            if let Some(ppt) = lv.get_property("name") {
                                Ok(ppt.as_str().unwrap() != "Angola")
                            } else {
                                Ok(false)
                            }
                        } else {
                            Ok(false)
                        }
                    })
                    .expect("build subtask error!")
            };

            if is_use_apply {
                stream = stream
                    .apply(|sub| subtask(sub).any())?
                    .filter_map(|(v, b)| if b { Ok(Some(v)) } else { Ok(None) })?
            } else {
                stream = stream
                    .map(|v| Ok((v, v)))?
                    .repartition(|(_, v)| Ok(*v))
                    .flat_map(|(key, id)| {
                        Ok(GRAPH
                            .get_out_vertices(id as DefaultId, Some(&vec![1]))
                            .map(move |v| (key, v.get_id() as u64)))
                    })?
                    .repartition(|(_, v)| Ok(*v))
                    .filter(|(_, v)| {
                        if let Some(lv) = GRAPH.get_vertex(*v as DefaultId) {
                            if let Some(ppt) = lv.get_property("name") {
                                Ok(ppt.as_str().unwrap() != "Angola")
                            } else {
                                Ok(false)
                            }
                        } else {
                            Ok(false)
                        }
                    })?
                    .map(|(key, _)| Ok(key))?
                    .dedup()?
            }

            stream.count()?.sink_into(output)
        }
    })
    .expect("run job failure;");

    for count in result.next().unwrap().iter() {
        println!("count: {:?}", count);
    }
}
