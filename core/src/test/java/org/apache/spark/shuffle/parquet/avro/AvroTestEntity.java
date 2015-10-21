/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package org.apache.spark.shuffle.parquet.avro;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class AvroTestEntity extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 6618460632626642454L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AvroTestEntity\",\"namespace\":\"org.apache.spark.shuffle.parquet.avro\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",\"string\"]},{\"name\":\"b\",\"type\":[\"null\",\"int\"]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence a;
  @Deprecated public java.lang.Integer b;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public AvroTestEntity() {}

  /**
   * All-args constructor.
   */
  public AvroTestEntity(java.lang.CharSequence a, java.lang.Integer b) {
    this.a = a;
    this.b = b;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return a;
    case 1: return b;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: a = (java.lang.CharSequence)value$; break;
    case 1: b = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'a' field.
   */
  public java.lang.CharSequence getA() {
    return a;
  }

  /**
   * Sets the value of the 'a' field.
   * @param value the value to set.
   */
  public void setA(java.lang.CharSequence value) {
    this.a = value;
  }

  /**
   * Gets the value of the 'b' field.
   */
  public java.lang.Integer getB() {
    return b;
  }

  /**
   * Sets the value of the 'b' field.
   * @param value the value to set.
   */
  public void setB(java.lang.Integer value) {
    this.b = value;
  }

  /** Creates a new AvroTestEntity RecordBuilder */
  public static org.apache.spark.shuffle.parquet.avro.AvroTestEntity.Builder newBuilder() {
    return new org.apache.spark.shuffle.parquet.avro.AvroTestEntity.Builder();
  }
  
  /** Creates a new AvroTestEntity RecordBuilder by copying an existing Builder */
  public static org.apache.spark.shuffle.parquet.avro.AvroTestEntity.Builder newBuilder(org.apache.spark.shuffle.parquet.avro.AvroTestEntity.Builder other) {
    return new org.apache.spark.shuffle.parquet.avro.AvroTestEntity.Builder(other);
  }
  
  /** Creates a new AvroTestEntity RecordBuilder by copying an existing AvroTestEntity instance */
  public static org.apache.spark.shuffle.parquet.avro.AvroTestEntity.Builder newBuilder(org.apache.spark.shuffle.parquet.avro.AvroTestEntity other) {
    return new org.apache.spark.shuffle.parquet.avro.AvroTestEntity.Builder(other);
  }
  
  /**
   * RecordBuilder for AvroTestEntity instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<AvroTestEntity>
    implements org.apache.avro.data.RecordBuilder<AvroTestEntity> {

    private java.lang.CharSequence a;
    private java.lang.Integer b;

    /** Creates a new Builder */
    private Builder() {
      super(org.apache.spark.shuffle.parquet.avro.AvroTestEntity.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(org.apache.spark.shuffle.parquet.avro.AvroTestEntity.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.a)) {
        this.a = data().deepCopy(fields()[0].schema(), other.a);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.b)) {
        this.b = data().deepCopy(fields()[1].schema(), other.b);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing AvroTestEntity instance */
    private Builder(org.apache.spark.shuffle.parquet.avro.AvroTestEntity other) {
            super(org.apache.spark.shuffle.parquet.avro.AvroTestEntity.SCHEMA$);
      if (isValidValue(fields()[0], other.a)) {
        this.a = data().deepCopy(fields()[0].schema(), other.a);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.b)) {
        this.b = data().deepCopy(fields()[1].schema(), other.b);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'a' field.
      */
    public java.lang.CharSequence getA() {
      return a;
    }

    /**
      * Sets the value of the 'a' field.
      * @param value the value to set.
      */
    public org.apache.spark.shuffle.parquet.avro.AvroTestEntity.Builder setA(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.a = value;
      fieldSetFlags()[0] = true;
      return this; 
    }

    /**
      * Checks whether the 'a' field has been set.
      */
    public boolean hasA() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'a' field.
      */
    public org.apache.spark.shuffle.parquet.avro.AvroTestEntity.Builder clearA() {
      a = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'b' field.
      */
    public java.lang.Integer getB() {
      return b;
    }

    /**
      * Sets the value of the 'b' field.
      * @param value the value to set.
      */
    public org.apache.spark.shuffle.parquet.avro.AvroTestEntity.Builder setB(java.lang.Integer value) {
      validate(fields()[1], value);
      this.b = value;
      fieldSetFlags()[1] = true;
      return this; 
    }

    /**
      * Checks whether the 'b' field has been set.
      */
    public boolean hasB() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'b' field.
      */
    public org.apache.spark.shuffle.parquet.avro.AvroTestEntity.Builder clearB() {
      b = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public AvroTestEntity build() {
      try {
        AvroTestEntity record = new AvroTestEntity();
        record.a = fieldSetFlags()[0] ? this.a : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.b = fieldSetFlags()[1] ? this.b : (java.lang.Integer) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  private static final org.apache.avro.io.DatumWriter
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);  

  private static final org.apache.avro.io.DatumReader
    READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);  

}
