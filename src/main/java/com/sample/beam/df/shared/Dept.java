/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.sample.beam.df.shared;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Dept extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -4528302175421258557L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Dept\",\"namespace\":\"com.sample.beam.df.shared\",\"fields\":[{\"name\":\"empid\",\"type\":[\"int\",\"null\"]},{\"name\":\"dept\",\"type\":[\"string\",\"null\"]},{\"name\":\"hireDate\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<Dept> ENCODER =
      new BinaryMessageEncoder<Dept>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<Dept> DECODER =
      new BinaryMessageDecoder<Dept>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<Dept> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<Dept> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<Dept>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this Dept to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a Dept from a ByteBuffer. */
  public static Dept fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public java.lang.Integer empid;
  @Deprecated public java.lang.CharSequence dept;
  @Deprecated public org.joda.time.LocalDate hireDate;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Dept() {}

  /**
   * All-args constructor.
   * @param empid The new value for empid
   * @param dept The new value for dept
   * @param hireDate The new value for hireDate
   */
  public Dept(java.lang.Integer empid, java.lang.CharSequence dept, org.joda.time.LocalDate hireDate) {
    this.empid = empid;
    this.dept = dept;
    this.hireDate = hireDate;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return empid;
    case 1: return dept;
    case 2: return hireDate;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  protected static final org.apache.avro.data.TimeConversions.DateConversion DATE_CONVERSION = new org.apache.avro.data.TimeConversions.DateConversion();
  protected static final org.apache.avro.data.TimeConversions.TimeConversion TIME_CONVERSION = new org.apache.avro.data.TimeConversions.TimeConversion();
  protected static final org.apache.avro.data.TimeConversions.TimestampConversion TIMESTAMP_CONVERSION = new org.apache.avro.data.TimeConversions.TimestampConversion();
  protected static final org.apache.avro.Conversions.DecimalConversion DECIMAL_CONVERSION = new org.apache.avro.Conversions.DecimalConversion();

  private static final org.apache.avro.Conversion<?>[] conversions =
      new org.apache.avro.Conversion<?>[] {
      null,
      null,
      DATE_CONVERSION,
      null
  };

  @Override
  public org.apache.avro.Conversion<?> getConversion(int field) {
    return conversions[field];
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: empid = (java.lang.Integer)value$; break;
    case 1: dept = (java.lang.CharSequence)value$; break;
    case 2: hireDate = (org.joda.time.LocalDate)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'empid' field.
   * @return The value of the 'empid' field.
   */
  public java.lang.Integer getEmpid() {
    return empid;
  }

  /**
   * Sets the value of the 'empid' field.
   * @param value the value to set.
   */
  public void setEmpid(java.lang.Integer value) {
    this.empid = value;
  }

  /**
   * Gets the value of the 'dept' field.
   * @return The value of the 'dept' field.
   */
  public java.lang.CharSequence getDept() {
    return dept;
  }

  /**
   * Sets the value of the 'dept' field.
   * @param value the value to set.
   */
  public void setDept(java.lang.CharSequence value) {
    this.dept = value;
  }

  /**
   * Gets the value of the 'hireDate' field.
   * @return The value of the 'hireDate' field.
   */
  public org.joda.time.LocalDate getHireDate() {
    return hireDate;
  }

  /**
   * Sets the value of the 'hireDate' field.
   * @param value the value to set.
   */
  public void setHireDate(org.joda.time.LocalDate value) {
    this.hireDate = value;
  }

  /**
   * Creates a new Dept RecordBuilder.
   * @return A new Dept RecordBuilder
   */
  public static com.sample.beam.df.shared.Dept.Builder newBuilder() {
    return new com.sample.beam.df.shared.Dept.Builder();
  }

  /**
   * Creates a new Dept RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Dept RecordBuilder
   */
  public static com.sample.beam.df.shared.Dept.Builder newBuilder(com.sample.beam.df.shared.Dept.Builder other) {
    return new com.sample.beam.df.shared.Dept.Builder(other);
  }

  /**
   * Creates a new Dept RecordBuilder by copying an existing Dept instance.
   * @param other The existing instance to copy.
   * @return A new Dept RecordBuilder
   */
  public static com.sample.beam.df.shared.Dept.Builder newBuilder(com.sample.beam.df.shared.Dept other) {
    return new com.sample.beam.df.shared.Dept.Builder(other);
  }

  /**
   * RecordBuilder for Dept instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Dept>
    implements org.apache.avro.data.RecordBuilder<Dept> {

    private java.lang.Integer empid;
    private java.lang.CharSequence dept;
    private org.joda.time.LocalDate hireDate;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.sample.beam.df.shared.Dept.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.empid)) {
        this.empid = data().deepCopy(fields()[0].schema(), other.empid);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.dept)) {
        this.dept = data().deepCopy(fields()[1].schema(), other.dept);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.hireDate)) {
        this.hireDate = data().deepCopy(fields()[2].schema(), other.hireDate);
        fieldSetFlags()[2] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing Dept instance
     * @param other The existing instance to copy.
     */
    private Builder(com.sample.beam.df.shared.Dept other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.empid)) {
        this.empid = data().deepCopy(fields()[0].schema(), other.empid);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.dept)) {
        this.dept = data().deepCopy(fields()[1].schema(), other.dept);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.hireDate)) {
        this.hireDate = data().deepCopy(fields()[2].schema(), other.hireDate);
        fieldSetFlags()[2] = true;
      }
    }

    /**
      * Gets the value of the 'empid' field.
      * @return The value.
      */
    public java.lang.Integer getEmpid() {
      return empid;
    }

    /**
      * Sets the value of the 'empid' field.
      * @param value The value of 'empid'.
      * @return This builder.
      */
    public com.sample.beam.df.shared.Dept.Builder setEmpid(java.lang.Integer value) {
      validate(fields()[0], value);
      this.empid = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'empid' field has been set.
      * @return True if the 'empid' field has been set, false otherwise.
      */
    public boolean hasEmpid() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'empid' field.
      * @return This builder.
      */
    public com.sample.beam.df.shared.Dept.Builder clearEmpid() {
      empid = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'dept' field.
      * @return The value.
      */
    public java.lang.CharSequence getDept() {
      return dept;
    }

    /**
      * Sets the value of the 'dept' field.
      * @param value The value of 'dept'.
      * @return This builder.
      */
    public com.sample.beam.df.shared.Dept.Builder setDept(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.dept = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'dept' field has been set.
      * @return True if the 'dept' field has been set, false otherwise.
      */
    public boolean hasDept() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'dept' field.
      * @return This builder.
      */
    public com.sample.beam.df.shared.Dept.Builder clearDept() {
      dept = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'hireDate' field.
      * @return The value.
      */
    public org.joda.time.LocalDate getHireDate() {
      return hireDate;
    }

    /**
      * Sets the value of the 'hireDate' field.
      * @param value The value of 'hireDate'.
      * @return This builder.
      */
    public com.sample.beam.df.shared.Dept.Builder setHireDate(org.joda.time.LocalDate value) {
      validate(fields()[2], value);
      this.hireDate = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'hireDate' field has been set.
      * @return True if the 'hireDate' field has been set, false otherwise.
      */
    public boolean hasHireDate() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'hireDate' field.
      * @return This builder.
      */
    public com.sample.beam.df.shared.Dept.Builder clearHireDate() {
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dept build() {
      try {
        Dept record = new Dept();
        record.empid = fieldSetFlags()[0] ? this.empid : (java.lang.Integer) defaultValue(fields()[0], record.getConversion(0));
        record.dept = fieldSetFlags()[1] ? this.dept : (java.lang.CharSequence) defaultValue(fields()[1], record.getConversion(1));
        record.hireDate = fieldSetFlags()[2] ? this.hireDate : (org.joda.time.LocalDate) defaultValue(fields()[2], record.getConversion(2));
        return record;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<Dept>
    WRITER$ = (org.apache.avro.io.DatumWriter<Dept>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<Dept>
    READER$ = (org.apache.avro.io.DatumReader<Dept>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}