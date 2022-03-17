/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.writable.kryo.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

/**
 * Kryo Serializer for Fastutil collection class. By default, because they extend boxed collections,
 * are being serialized very inefficiently through a lot of temporary object creation.
 * <p>
 * We are relying that fastutil classes are written to be correctly serialized with Java
 * serialization, and have put transient on all array fields and are doing custom efficient
 * serialization in writeObject/readObject methods. This Serializer then swaps ObjectOutputStream
 * for all default fields with FieldSerializer, and then calls appropriate writeObject/readObject
 * methods. We are also relying on defaultWriteObject/defaultReadObject being effectively called
 * first within those methods
 *
 * @param <T> Object type
 */
public class FastUtilSerializer<T> extends Serializer<T> {

    /**
     * List of all types generated by fastutil
     */
    private static final String[] PRIMITIVE_TYPES =
            new String[] {
                "Boolean", "Byte", "Short", "Int", "Long", "Float", "Double", "Char", "Object"
            };
    /**
     * List of all types used as keys in fastutil
     */
    private static final String[] PRIMITIVE_KEY_TYPES =
            new String[] {"Byte", "Short", "Int", "Long", "Float", "Double", "Char", "Object"};

    /**
     * Field serializer for this fastutil class
     */
    private final FieldSerializer<T> fieldSerializer;

    /**
     * Handle to writeObject Method on this fastutil class
     */
    private final Method writeMethod;
    /**
     * Handle to readObject Method on this fastutil class
     */
    private final Method readMethod;
    /**
     * Reusable output stream wrapper
     */
    private final FastUtilSerializer.FastutilKryoObjectOutputStream outputWrapper;
    /**
     * Reusable input stream wrapper
     */
    private final FastUtilSerializer.FastutilKryoObjectInputStream inputWrapper;

    /**
     * Creates and initializes new serializer for a given fastutil class.
     *
     * @param kryo Kryo instance
     * @param type Fastutil class
     */
    public FastUtilSerializer(Kryo kryo, Class<T> type) {
        fieldSerializer = new FieldSerializer<>(kryo, type);
        fieldSerializer.setIgnoreSyntheticFields(false);

        try {
            writeMethod = type.getDeclaredMethod("writeObject", ObjectOutputStream.class);
            writeMethod.setAccessible(true);
            readMethod = type.getDeclaredMethod("readObject", ObjectInputStream.class);
            readMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(
                    "Fastutil class " + type + " doesn't have readObject/writeObject methods", e);
        }

        try {
            outputWrapper = new FastutilKryoObjectOutputStream();
            inputWrapper = new FastutilKryoObjectInputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Register serializer for a given fastutil class.
     *
     * @param kryo          Kryo instance
     * @param fastutilClass Fastutil class
     */
    public static void register(Kryo kryo, Class<?> fastutilClass) {
        kryo.register(fastutilClass, new FastUtilSerializer<>(kryo, fastutilClass));
    }

    /**
     * Register all Fastutil ArrayLists.
     *
     * @param kryo Kryo instance
     */
    public static void registerArrayLists(Kryo kryo) {
        registerAll(
                kryo, singleTypes("it.unimi.dsi.fastutil._t1_s._T1_ArrayList", PRIMITIVE_TYPES));
    }

    /**
     * Register all Fastutil ArrayBigLists.
     *
     * @param kryo Kryo instance
     */
    public static void registerArrayBigList(Kryo kryo) {
        registerAll(
                kryo,
                singleTypes("it.unimi.dsi.fastutil._t1_s._T1_BigArrayBigList", PRIMITIVE_TYPES));
    }

    /**
     * Register all Fastutil OpenHashSets.
     *
     * @param kryo Kryo instance
     */
    public static void registerOpenHashSets(Kryo kryo) {
        registerAll(
                kryo, singleTypes("it.unimi.dsi.fastutil._t1_s._T1_OpenHashSet", PRIMITIVE_TYPES));
    }

    /**
     * Register all Fastutil ArraySets.
     *
     * @param kryo Kryo instance
     */
    public static void registerArraySets(Kryo kryo) {
        registerAll(kryo, singleTypes("it.unimi.dsi.fastutil._t1_s._T1_ArraySet", PRIMITIVE_TYPES));
    }

    /**
     * Register all Fastutil RBTreeSets.
     *
     * @param kryo Kryo instance
     */
    public static void registerRBTreeSets(Kryo kryo) {
        registerAll(
                kryo,
                singleTypes("it.unimi.dsi.fastutil._t1_s._T1_RBTreeSet", PRIMITIVE_KEY_TYPES));
    }

    /**
     * Register all Fastutil AVLTreeSets.
     *
     * @param kryo Kryo instance
     */
    public static void registerAVLTreeSets(Kryo kryo) {
        registerAll(
                kryo,
                singleTypes("it.unimi.dsi.fastutil._t1_s._T1_AVLTreeSet", PRIMITIVE_KEY_TYPES));
    }

    /**
     * Register all Fastutil OpenHashMaps.
     *
     * @param kryo Kryo instance
     */
    public static void registerOpenHashMaps(Kryo kryo) {
        registerAll(
                kryo,
                doubleTypes(
                        "it.unimi.dsi.fastutil._t1_s._T1_2_T2_OpenHashMap",
                        PRIMITIVE_KEY_TYPES,
                        PRIMITIVE_TYPES));
    }

    /**
     * Register all Fastutil RBTreeMaps.
     *
     * @param kryo Kryo instance
     */
    public static void registerRBTreeMaps(Kryo kryo) {
        registerAll(
                kryo,
                doubleTypes(
                        "it.unimi.dsi.fastutil._t1_s._T1_2_T2_RBTreeMap",
                        PRIMITIVE_KEY_TYPES,
                        PRIMITIVE_TYPES));
    }

    /**
     * Register all Fastutil AVLTreeMaps.
     *
     * @param kryo Kryo instance
     */
    public static void registerAVLTreeMaps(Kryo kryo) {
        registerAll(
                kryo,
                doubleTypes(
                        "it.unimi.dsi.fastutil._t1_s._T1_2_T2_AVLTreeMap",
                        PRIMITIVE_KEY_TYPES,
                        PRIMITIVE_TYPES));
    }

    /**
     * Registers serializers for all possible fastutil classes.
     * <p>
     * There are many fastutil classes, so it is recommended to call this function at the end, so
     * they fastutil classes don't use up small registration numbers.
     *
     * @param kryo Kryo instance
     */
    public static void registerAll(Kryo kryo) {
        registerArrayLists(kryo);
        registerArrayBigList(kryo);
        registerOpenHashSets(kryo);
        registerArraySets(kryo);
        registerRBTreeSets(kryo);
        registerAVLTreeSets(kryo);
        registerOpenHashMaps(kryo);
        registerRBTreeMaps(kryo);
        registerAVLTreeMaps(kryo);

        // Note - HeapPriorityQueues don't extend boxed collection,
        // and so they work out of the box correctly
    }

    /**
     * Register all class from the list of classes.
     *
     * @param kryo  Kryo instance
     * @param types List of classes
     */
    private static void registerAll(Kryo kryo, ArrayList<Class<?>> types) {
        for (Class<?> type : types) {
            register(kryo, type);
        }
    }

    /**
     * Returns list of all classes that are generated by using given pattern, and replacing it with
     * passed list of types. Pattern contains _t1_ and _T1_, for lowercase and actual name.
     *
     * @param pattern Given pattern
     * @param types   Given list of strings to replace into pattern
     * @return List of all classes
     */
    private static ArrayList<Class<?>> singleTypes(String pattern, String[] types) {
        ArrayList<Class<?>> result = new ArrayList<>();

        for (String type : types) {
            try {
                result.add(
                        Class.forName(
                                pattern.replaceAll("_T1_", type)
                                        .replaceAll("_t1_", type.toLowerCase())));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(pattern + " " + type, e);
            }
        }
        return result;
    }

    /**
     * Returns list of all classes that are generated by using given pattern, and replacing it with
     * passed list of types. Pattern contains two variable pairs: _t1_, _T1_ and _t2_, _T2_, in each
     * pair one for lowercase and one for actual name.
     *
     * @param pattern Given pattern
     * @param types1  Given list of strings to replace t1 into pattern
     * @param types2  Given list of strings to replace t2 into pattern
     * @return List of all classes
     */
    private static ArrayList<Class<?>> doubleTypes(
            String pattern, String[] types1, String[] types2) {
        ArrayList<Class<?>> result = new ArrayList<>();

        for (String type1 : types1) {
            for (String type2 : types2) {
                try {
                    result.add(
                            Class.forName(
                                    pattern.replaceAll("_T1_", type1)
                                            .replaceAll("_t1_", type1.toLowerCase())
                                            .replaceAll("_T2_", type2)
                                            .replaceAll("_t2_", type2.toLowerCase())));
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(pattern + " " + type1 + " " + type2, e);
                }
            }
        }
        return result;
    }

    @Override
    public void write(Kryo kryo, Output output, T object) {
        fieldSerializer.write(kryo, output, object);

        outputWrapper.set(output, kryo);
        try {
            writeMethod.invoke(object, outputWrapper);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("writeObject failed", e);
        }
    }

    @Override
    public T read(Kryo kryo, Input input, Class<T> type) {
        T result = fieldSerializer.read(kryo, input, type);

        if (result != null) {
            inputWrapper.set(input, kryo);
            try {
                readMethod.invoke(result, inputWrapper);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException("readObject failed", e);
            }
        }

        return result;
    }

    /**
     * Wrapper around ObjectOutputStream that ignores defaultWriteObject (assumes that needed logic
     * was already executed before), and passes all other calls to Output
     */
    private static class FastutilKryoObjectOutputStream extends ObjectOutputStream {

        /**
         * Output
         */
        private Output output;
        /**
         * Kryo
         */
        private Kryo kryo;

        /**
         * Constructor
         */
        FastutilKryoObjectOutputStream() throws IOException {
            super();
        }

        /**
         * Setter
         *
         * @param output Output
         * @param kryo   kryo
         */
        public void set(Output output, Kryo kryo) {
            this.output = output;
            this.kryo = kryo;
        }

        @Override
        public void defaultWriteObject() throws IOException {}

        @Override
        public void writeBoolean(boolean val) throws IOException {
            output.writeBoolean(val);
        }

        @Override
        public void writeByte(int val) throws IOException {
            output.writeByte(val);
        }

        @Override
        public void writeShort(int val) throws IOException {
            output.writeShort(val);
        }

        @Override
        public void writeChar(int val) throws IOException {
            output.writeChar((char) val);
        }

        @Override
        public void writeInt(int val) throws IOException {
            output.writeInt(val, false);
        }

        @Override
        public void writeLong(long val) throws IOException {
            output.writeLong(val, false);
        }

        @Override
        public void writeFloat(float val) throws IOException {
            output.writeFloat(val);
        }

        @Override
        public void writeDouble(double val) throws IOException {
            output.writeDouble(val);
        }

        @Override
        protected void writeObjectOverride(Object obj) throws IOException {
            kryo.writeClassAndObject(output, obj);
        }
    }

    /**
     * Wrapper around ObjectOutputStream that ignores defaultReadObject (assumes that needed logic
     * was already executed before), and passes all other calls to Output
     */
    private static class FastutilKryoObjectInputStream extends ObjectInputStream {

        /**
         * Input
         */
        private Input input;
        /**
         * Kryo
         */
        private Kryo kryo;

        /**
         * Constructor
         */
        FastutilKryoObjectInputStream() throws IOException {
            super();
        }

        /**
         * Setter
         *
         * @param input Input
         * @param kryo  Kryo
         */
        public void set(Input input, Kryo kryo) {
            this.input = input;
            this.kryo = kryo;
        }

        @Override
        public void defaultReadObject() throws IOException, ClassNotFoundException {}

        @Override
        public boolean readBoolean() throws IOException {
            return input.readBoolean();
        }

        @Override
        public byte readByte() throws IOException {
            return input.readByte();
        }

        @Override
        public char readChar() throws IOException {
            return input.readChar();
        }

        @Override
        public short readShort() throws IOException {
            return input.readShort();
        }

        @Override
        public int readInt() throws IOException {
            return input.readInt(false);
        }

        @Override
        public long readLong() throws IOException {
            return input.readLong(false);
        }

        @Override
        public float readFloat() throws IOException {
            return input.readFloat();
        }

        @Override
        public double readDouble() throws IOException {
            return input.readDouble();
        }

        @Override
        protected Object readObjectOverride() throws IOException, ClassNotFoundException {
            return kryo.readClassAndObject(input);
        }
    }
}