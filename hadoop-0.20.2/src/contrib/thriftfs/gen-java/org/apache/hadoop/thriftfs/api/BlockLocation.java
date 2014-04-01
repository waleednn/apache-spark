/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.hadoop.thriftfs.api;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import com.facebook.thrift.*;

import com.facebook.thrift.protocol.*;
import com.facebook.thrift.transport.*;

public class BlockLocation implements TBase, java.io.Serializable {
  public List<String> hosts;
  public List<String> names;
  public long offset;
  public long length;

  public final Isset __isset = new Isset();
  public static final class Isset implements java.io.Serializable {
    public boolean hosts = false;
    public boolean names = false;
    public boolean offset = false;
    public boolean length = false;
  }

  public BlockLocation() {
  }

  public BlockLocation(
    List<String> hosts,
    List<String> names,
    long offset,
    long length)
  {
    this();
    this.hosts = hosts;
    this.__isset.hosts = true;
    this.names = names;
    this.__isset.names = true;
    this.offset = offset;
    this.__isset.offset = true;
    this.length = length;
    this.__isset.length = true;
  }

  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof BlockLocation)
      return this.equals((BlockLocation)that);
    return false;
  }

  public boolean equals(BlockLocation that) {
    if (that == null)
      return false;

    boolean this_present_hosts = true && (this.hosts != null);
    boolean that_present_hosts = true && (that.hosts != null);
    if (this_present_hosts || that_present_hosts) {
      if (!(this_present_hosts && that_present_hosts))
        return false;
      if (!this.hosts.equals(that.hosts))
        return false;
    }

    boolean this_present_names = true && (this.names != null);
    boolean that_present_names = true && (that.names != null);
    if (this_present_names || that_present_names) {
      if (!(this_present_names && that_present_names))
        return false;
      if (!this.names.equals(that.names))
        return false;
    }

    boolean this_present_offset = true;
    boolean that_present_offset = true;
    if (this_present_offset || that_present_offset) {
      if (!(this_present_offset && that_present_offset))
        return false;
      if (this.offset != that.offset)
        return false;
    }

    boolean this_present_length = true;
    boolean that_present_length = true;
    if (this_present_length || that_present_length) {
      if (!(this_present_length && that_present_length))
        return false;
      if (this.length != that.length)
        return false;
    }

    return true;
  }

  public int hashCode() {
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id)
      {
        case 1:
          if (field.type == TType.LIST) {
            {
              TList _list0 = iprot.readListBegin();
              this.hosts = new ArrayList<String>(_list0.size);
              for (int _i1 = 0; _i1 < _list0.size; ++_i1)
              {
                String _elem2 = null;
                _elem2 = iprot.readString();
                this.hosts.add(_elem2);
              }
              iprot.readListEnd();
            }
            this.__isset.hosts = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2:
          if (field.type == TType.LIST) {
            {
              TList _list3 = iprot.readListBegin();
              this.names = new ArrayList<String>(_list3.size);
              for (int _i4 = 0; _i4 < _list3.size; ++_i4)
              {
                String _elem5 = null;
                _elem5 = iprot.readString();
                this.names.add(_elem5);
              }
              iprot.readListEnd();
            }
            this.__isset.names = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 3:
          if (field.type == TType.I64) {
            this.offset = iprot.readI64();
            this.__isset.offset = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 4:
          if (field.type == TType.I64) {
            this.length = iprot.readI64();
            this.__isset.length = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
  }

  public void write(TProtocol oprot) throws TException {
    TStruct struct = new TStruct("BlockLocation");
    oprot.writeStructBegin(struct);
    TField field = new TField();
    if (this.hosts != null) {
      field.name = "hosts";
      field.type = TType.LIST;
      field.id = 1;
      oprot.writeFieldBegin(field);
      {
        oprot.writeListBegin(new TList(TType.STRING, this.hosts.size()));
        for (String _iter6 : this.hosts)        {
          oprot.writeString(_iter6);
        }
        oprot.writeListEnd();
      }
      oprot.writeFieldEnd();
    }
    if (this.names != null) {
      field.name = "names";
      field.type = TType.LIST;
      field.id = 2;
      oprot.writeFieldBegin(field);
      {
        oprot.writeListBegin(new TList(TType.STRING, this.names.size()));
        for (String _iter7 : this.names)        {
          oprot.writeString(_iter7);
        }
        oprot.writeListEnd();
      }
      oprot.writeFieldEnd();
    }
    field.name = "offset";
    field.type = TType.I64;
    field.id = 3;
    oprot.writeFieldBegin(field);
    oprot.writeI64(this.offset);
    oprot.writeFieldEnd();
    field.name = "length";
    field.type = TType.I64;
    field.id = 4;
    oprot.writeFieldBegin(field);
    oprot.writeI64(this.length);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("BlockLocation(");
    sb.append("hosts:");
    sb.append(this.hosts);
    sb.append(",names:");
    sb.append(this.names);
    sb.append(",offset:");
    sb.append(this.offset);
    sb.append(",length:");
    sb.append(this.length);
    sb.append(")");
    return sb.toString();
  }

}

