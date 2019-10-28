/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hive.service.rpc.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
public class TOperationHandle implements org.apache.thrift.TBase<TOperationHandle, TOperationHandle._Fields>, java.io.Serializable, Cloneable, Comparable<TOperationHandle> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TOperationHandle");

  private static final org.apache.thrift.protocol.TField OPERATION_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("operationId", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField OPERATION_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("operationType", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField HAS_RESULT_SET_FIELD_DESC = new org.apache.thrift.protocol.TField("hasResultSet", org.apache.thrift.protocol.TType.BOOL, (short)3);
  private static final org.apache.thrift.protocol.TField MODIFIED_ROW_COUNT_FIELD_DESC = new org.apache.thrift.protocol.TField("modifiedRowCount", org.apache.thrift.protocol.TType.DOUBLE, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TOperationHandleStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TOperationHandleTupleSchemeFactory());
  }

  private THandleIdentifier operationId; // required
  private TOperationType operationType; // required
  private boolean hasResultSet; // required
  private double modifiedRowCount; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    OPERATION_ID((short)1, "operationId"),
    /**
     * 
     * @see TOperationType
     */
    OPERATION_TYPE((short)2, "operationType"),
    HAS_RESULT_SET((short)3, "hasResultSet"),
    MODIFIED_ROW_COUNT((short)4, "modifiedRowCount");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // OPERATION_ID
          return OPERATION_ID;
        case 2: // OPERATION_TYPE
          return OPERATION_TYPE;
        case 3: // HAS_RESULT_SET
          return HAS_RESULT_SET;
        case 4: // MODIFIED_ROW_COUNT
          return MODIFIED_ROW_COUNT;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __HASRESULTSET_ISSET_ID = 0;
  private static final int __MODIFIEDROWCOUNT_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.MODIFIED_ROW_COUNT};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.OPERATION_ID, new org.apache.thrift.meta_data.FieldMetaData("operationId", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, THandleIdentifier.class)));
    tmpMap.put(_Fields.OPERATION_TYPE, new org.apache.thrift.meta_data.FieldMetaData("operationType", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, TOperationType.class)));
    tmpMap.put(_Fields.HAS_RESULT_SET, new org.apache.thrift.meta_data.FieldMetaData("hasResultSet", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.MODIFIED_ROW_COUNT, new org.apache.thrift.meta_data.FieldMetaData("modifiedRowCount", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TOperationHandle.class, metaDataMap);
  }

  public TOperationHandle() {
  }

  public TOperationHandle(
    THandleIdentifier operationId,
    TOperationType operationType,
    boolean hasResultSet)
  {
    this();
    this.operationId = operationId;
    this.operationType = operationType;
    this.hasResultSet = hasResultSet;
    setHasResultSetIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TOperationHandle(TOperationHandle other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetOperationId()) {
      this.operationId = new THandleIdentifier(other.operationId);
    }
    if (other.isSetOperationType()) {
      this.operationType = other.operationType;
    }
    this.hasResultSet = other.hasResultSet;
    this.modifiedRowCount = other.modifiedRowCount;
  }

  public TOperationHandle deepCopy() {
    return new TOperationHandle(this);
  }

  @Override
  public void clear() {
    this.operationId = null;
    this.operationType = null;
    setHasResultSetIsSet(false);
    this.hasResultSet = false;
    setModifiedRowCountIsSet(false);
    this.modifiedRowCount = 0.0;
  }

  public THandleIdentifier getOperationId() {
    return this.operationId;
  }

  public void setOperationId(THandleIdentifier operationId) {
    this.operationId = operationId;
  }

  public void unsetOperationId() {
    this.operationId = null;
  }

  /** Returns true if field operationId is set (has been assigned a value) and false otherwise */
  public boolean isSetOperationId() {
    return this.operationId != null;
  }

  public void setOperationIdIsSet(boolean value) {
    if (!value) {
      this.operationId = null;
    }
  }

  /**
   * 
   * @see TOperationType
   */
  public TOperationType getOperationType() {
    return this.operationType;
  }

  /**
   * 
   * @see TOperationType
   */
  public void setOperationType(TOperationType operationType) {
    this.operationType = operationType;
  }

  public void unsetOperationType() {
    this.operationType = null;
  }

  /** Returns true if field operationType is set (has been assigned a value) and false otherwise */
  public boolean isSetOperationType() {
    return this.operationType != null;
  }

  public void setOperationTypeIsSet(boolean value) {
    if (!value) {
      this.operationType = null;
    }
  }

  public boolean isHasResultSet() {
    return this.hasResultSet;
  }

  public void setHasResultSet(boolean hasResultSet) {
    this.hasResultSet = hasResultSet;
    setHasResultSetIsSet(true);
  }

  public void unsetHasResultSet() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __HASRESULTSET_ISSET_ID);
  }

  /** Returns true if field hasResultSet is set (has been assigned a value) and false otherwise */
  public boolean isSetHasResultSet() {
    return EncodingUtils.testBit(__isset_bitfield, __HASRESULTSET_ISSET_ID);
  }

  public void setHasResultSetIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __HASRESULTSET_ISSET_ID, value);
  }

  public double getModifiedRowCount() {
    return this.modifiedRowCount;
  }

  public void setModifiedRowCount(double modifiedRowCount) {
    this.modifiedRowCount = modifiedRowCount;
    setModifiedRowCountIsSet(true);
  }

  public void unsetModifiedRowCount() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __MODIFIEDROWCOUNT_ISSET_ID);
  }

  /** Returns true if field modifiedRowCount is set (has been assigned a value) and false otherwise */
  public boolean isSetModifiedRowCount() {
    return EncodingUtils.testBit(__isset_bitfield, __MODIFIEDROWCOUNT_ISSET_ID);
  }

  public void setModifiedRowCountIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __MODIFIEDROWCOUNT_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case OPERATION_ID:
      if (value == null) {
        unsetOperationId();
      } else {
        setOperationId((THandleIdentifier)value);
      }
      break;

    case OPERATION_TYPE:
      if (value == null) {
        unsetOperationType();
      } else {
        setOperationType((TOperationType)value);
      }
      break;

    case HAS_RESULT_SET:
      if (value == null) {
        unsetHasResultSet();
      } else {
        setHasResultSet((Boolean)value);
      }
      break;

    case MODIFIED_ROW_COUNT:
      if (value == null) {
        unsetModifiedRowCount();
      } else {
        setModifiedRowCount((Double)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case OPERATION_ID:
      return getOperationId();

    case OPERATION_TYPE:
      return getOperationType();

    case HAS_RESULT_SET:
      return isHasResultSet();

    case MODIFIED_ROW_COUNT:
      return getModifiedRowCount();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case OPERATION_ID:
      return isSetOperationId();
    case OPERATION_TYPE:
      return isSetOperationType();
    case HAS_RESULT_SET:
      return isSetHasResultSet();
    case MODIFIED_ROW_COUNT:
      return isSetModifiedRowCount();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TOperationHandle)
      return this.equals((TOperationHandle)that);
    return false;
  }

  public boolean equals(TOperationHandle that) {
    if (that == null)
      return false;

    boolean this_present_operationId = true && this.isSetOperationId();
    boolean that_present_operationId = true && that.isSetOperationId();
    if (this_present_operationId || that_present_operationId) {
      if (!(this_present_operationId && that_present_operationId))
        return false;
      if (!this.operationId.equals(that.operationId))
        return false;
    }

    boolean this_present_operationType = true && this.isSetOperationType();
    boolean that_present_operationType = true && that.isSetOperationType();
    if (this_present_operationType || that_present_operationType) {
      if (!(this_present_operationType && that_present_operationType))
        return false;
      if (!this.operationType.equals(that.operationType))
        return false;
    }

    boolean this_present_hasResultSet = true;
    boolean that_present_hasResultSet = true;
    if (this_present_hasResultSet || that_present_hasResultSet) {
      if (!(this_present_hasResultSet && that_present_hasResultSet))
        return false;
      if (this.hasResultSet != that.hasResultSet)
        return false;
    }

    boolean this_present_modifiedRowCount = true && this.isSetModifiedRowCount();
    boolean that_present_modifiedRowCount = true && that.isSetModifiedRowCount();
    if (this_present_modifiedRowCount || that_present_modifiedRowCount) {
      if (!(this_present_modifiedRowCount && that_present_modifiedRowCount))
        return false;
      if (this.modifiedRowCount != that.modifiedRowCount)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_operationId = true && (isSetOperationId());
    list.add(present_operationId);
    if (present_operationId)
      list.add(operationId);

    boolean present_operationType = true && (isSetOperationType());
    list.add(present_operationType);
    if (present_operationType)
      list.add(operationType.getValue());

    boolean present_hasResultSet = true;
    list.add(present_hasResultSet);
    if (present_hasResultSet)
      list.add(hasResultSet);

    boolean present_modifiedRowCount = true && (isSetModifiedRowCount());
    list.add(present_modifiedRowCount);
    if (present_modifiedRowCount)
      list.add(modifiedRowCount);

    return list.hashCode();
  }

  @Override
  public int compareTo(TOperationHandle other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetOperationId()).compareTo(other.isSetOperationId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOperationId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.operationId, other.operationId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetOperationType()).compareTo(other.isSetOperationType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOperationType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.operationType, other.operationType);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetHasResultSet()).compareTo(other.isSetHasResultSet());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetHasResultSet()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.hasResultSet, other.hasResultSet);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetModifiedRowCount()).compareTo(other.isSetModifiedRowCount());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetModifiedRowCount()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.modifiedRowCount, other.modifiedRowCount);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TOperationHandle(");
    boolean first = true;

    sb.append("operationId:");
    if (this.operationId == null) {
      sb.append("null");
    } else {
      sb.append(this.operationId);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("operationType:");
    if (this.operationType == null) {
      sb.append("null");
    } else {
      sb.append(this.operationType);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("hasResultSet:");
    sb.append(this.hasResultSet);
    first = false;
    if (isSetModifiedRowCount()) {
      if (!first) sb.append(", ");
      sb.append("modifiedRowCount:");
      sb.append(this.modifiedRowCount);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetOperationId()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'operationId' is unset! Struct:" + toString());
    }

    if (!isSetOperationType()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'operationType' is unset! Struct:" + toString());
    }

    if (!isSetHasResultSet()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'hasResultSet' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (operationId != null) {
      operationId.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TOperationHandleStandardSchemeFactory implements SchemeFactory {
    public TOperationHandleStandardScheme getScheme() {
      return new TOperationHandleStandardScheme();
    }
  }

  private static class TOperationHandleStandardScheme extends StandardScheme<TOperationHandle> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TOperationHandle struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // OPERATION_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.operationId = new THandleIdentifier();
              struct.operationId.read(iprot);
              struct.setOperationIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // OPERATION_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.operationType = org.apache.hive.service.rpc.thrift.TOperationType.findByValue(iprot.readI32());
              struct.setOperationTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // HAS_RESULT_SET
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.hasResultSet = iprot.readBool();
              struct.setHasResultSetIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // MODIFIED_ROW_COUNT
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.modifiedRowCount = iprot.readDouble();
              struct.setModifiedRowCountIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TOperationHandle struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.operationId != null) {
        oprot.writeFieldBegin(OPERATION_ID_FIELD_DESC);
        struct.operationId.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.operationType != null) {
        oprot.writeFieldBegin(OPERATION_TYPE_FIELD_DESC);
        oprot.writeI32(struct.operationType.getValue());
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(HAS_RESULT_SET_FIELD_DESC);
      oprot.writeBool(struct.hasResultSet);
      oprot.writeFieldEnd();
      if (struct.isSetModifiedRowCount()) {
        oprot.writeFieldBegin(MODIFIED_ROW_COUNT_FIELD_DESC);
        oprot.writeDouble(struct.modifiedRowCount);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TOperationHandleTupleSchemeFactory implements SchemeFactory {
    public TOperationHandleTupleScheme getScheme() {
      return new TOperationHandleTupleScheme();
    }
  }

  private static class TOperationHandleTupleScheme extends TupleScheme<TOperationHandle> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TOperationHandle struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      struct.operationId.write(oprot);
      oprot.writeI32(struct.operationType.getValue());
      oprot.writeBool(struct.hasResultSet);
      BitSet optionals = new BitSet();
      if (struct.isSetModifiedRowCount()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetModifiedRowCount()) {
        oprot.writeDouble(struct.modifiedRowCount);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TOperationHandle struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.operationId = new THandleIdentifier();
      struct.operationId.read(iprot);
      struct.setOperationIdIsSet(true);
      struct.operationType = org.apache.hive.service.rpc.thrift.TOperationType.findByValue(iprot.readI32());
      struct.setOperationTypeIsSet(true);
      struct.hasResultSet = iprot.readBool();
      struct.setHasResultSetIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.modifiedRowCount = iprot.readDouble();
        struct.setModifiedRowCountIsSet(true);
      }
    }
  }

}

