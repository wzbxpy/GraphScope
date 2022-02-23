#[macro_use]
extern crate lazy_static;

use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::prelude::*;
use pegasus::api::{CorrelatedSubTask, Dedup, Filter, HasAny, Map, Merge, Sink};
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
    #[structopt(
        short = "i",
        long = "starters",
        default_value = "72088380363511554",
        use_delimiter = true
    )]
    starter: Vec<u64>,
    #[structopt(short = "w", long = "workers", default_value = "2")]
    workers: u32,
    #[structopt(short = "s", long = "startdate", default_value = "20110601000000000")]
    start_date: u64,
    #[structopt(short = "t", long = "enddate", default_value = "20110901000000000")]
    end_date: u64,
    #[structopt(short = "x", long = "xcountry", default_value = "Laos")]
    country_x_name: String,
    #[structopt(short = "y", long = "ycountry", default_value = "Scotland")]
    country_y_name: String,
}

fn main() {
    let config: Config = Config::from_args();
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();

    let mut conf = JobConf::new("ldbc_cr3");
    conf.set_workers(config.workers);

    let mut result = pegasus::run(conf, || {
        let id = pegasus::get_current_worker().index;
        let starters = config.starter.clone();
        let start_date = config.start_date;
        let end_date = config.end_date;
        let country_x_name = config.country_x_name.clone();
        let country_y_name = config.country_y_name.clone();

        move |input, output| {
            let start = if id == 0 {
                starters.into_iter()
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
                .dedup()?;

            let subtask = |input: Stream<u64>| {
                let country_x_name = country_x_name.clone();
                let country_y_name = country_y_name.clone();
                input
                    .repartition(|id| Ok(*id))
                    .flat_map(|id| {
                        Ok(GRAPH
                            .get_out_vertices(id as DefaultId, Some(&vec![11]))
                            .map(|v| v.get_id() as u64))
                    })
                    .expect("build subtask error!")
                    .repartition(|v| Ok(*v))
                    .filter(move |v| {
                        if let Some(lv) = GRAPH.get_vertex(*v as DefaultId) {
                            if let Some(ppt) = lv.get_property("name") {
                                Ok(ppt.as_str().unwrap() != country_x_name
                                    && ppt.as_str().unwrap() != country_y_name)
                            } else {
                                Ok(false)
                            }
                        } else {
                            Ok(false)
                        }
                    })
                    .expect("build subtask error!")
            };

            let subtask2 = |input: Stream<u64>| {
                let start_date = start_date.clone();
                let end_date = end_date.clone();
                let country_x_name = country_x_name.clone();
                let country_y_name = country_y_name.clone();
                input
                    .repartition(|id| Ok(*id))
                    .flat_map(|id| {
                        Ok(GRAPH
                            .get_in_vertices(id as DefaultId, Some(&vec![0]))
                            .map(|v| v.get_id() as u64))
                    })
                    .expect("build subtask error!")
                    .repartition(|v| Ok(*v))
                    .filter(move |v| {
                        if let Some(lv) = GRAPH.get_vertex(*v as DefaultId) {
                            if let Some(ppt) = lv.get_property("creationDate") {
                                Ok(ppt.as_u64().unwrap() > start_date
                                    && ppt.as_u64().unwrap() < end_date)
                            } else {
                                Ok(false)
                            }
                        } else {
                            Ok(false)
                        }
                    })
                    .expect("build subtask error!")
                    .repartition(|id| Ok(*id))
                    .flat_map(|id| {
                        Ok(GRAPH
                            .get_out_vertices(id as DefaultId, Some(&vec![11]))
                            .map(|v| v.get_id() as u64))
                    })
                    .expect("build subtask error!")
                    .repartition(|v| Ok(*v))
                    .filter(move |v| {
                        if let Some(lv) = GRAPH.get_vertex(*v as DefaultId) {
                            if let Some(ppt) = lv.get_property("name") {
                                Ok(ppt.as_str().unwrap().eq(&country_x_name)
                                    || ppt.as_str().unwrap().eq(&country_y_name))
                            } else {
                                Ok(false)
                            }
                        } else {
                            Ok(false)
                        }
                    })
                    .expect("build subtask error!")
            };

            stream = stream
                .apply(|sub| subtask(sub).any())?
                .filter_map(|(v, b)| if b { Ok(Some(v)) } else { Ok(None) })?
                .apply(|sub| subtask2(sub).any())?
                .filter_map(|(v, b)| if b { Ok(Some(v)) } else { Ok(None) })?;

            stream.sink_into(output)
        }
    })
    .expect("run job failure;");

    for res in result.next().unwrap().iter() {
        println!("result: {:?}", res);
    }
}
