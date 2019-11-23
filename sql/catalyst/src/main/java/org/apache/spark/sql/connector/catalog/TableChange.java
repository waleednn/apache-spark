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

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.types.DataType;

import java.util.Arrays;
import java.util.Objects;

/**
 * TableChange subclasses represent requested changes to a table. These are passed to
 * {@link TableCatalog#alterTable}. For example,
 * <pre>
 *   import TableChange._
 *   val catalog = Catalogs.load(name)
 *   catalog.asTableCatalog.alterTable(ident,
 *       addColumn("x", IntegerType),
 *       renameColumn("a", "b"),
 *       deleteColumn("c")
 *     )
 * </pre>
 */
@Experimental
public interface TableChange {

  /**
   * Create a TableChange for setting a table property.
   * <p>
   * If the property already exists, it will be replaced with the new value.
   *
   * @param property the property name
   * @param value the new property value
   * @return a TableChange for the addition
   */
  static TableChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Create a TableChange for removing a table property.
   * <p>
   * If the property does not exist, the change will succeed.
   *
   * @param property the property name
   * @return a TableChange for the addition
   */
  static TableChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /**
   * Create a TableChange for adding an optional column.
   * <p>
   * If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the new column
   * @param dataType the new column's data type
   * @return a TableChange for the addition
   */
  static TableChange addColumn(String[] fieldNames, DataType dataType) {
    return new AddColumn(fieldNames, dataType, true, null);
  }

  /**
   * Create a TableChange for adding a column.
   * <p>
   * If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the new column
   * @param dataType the new column's data type
   * @param isNullable whether the new column can contain null
   * @return a TableChange for the addition
   */
  static TableChange addColumn(String[] fieldNames, DataType dataType, boolean isNullable) {
    return new AddColumn(fieldNames, dataType, isNullable, null);
  }

  /**
   * Create a TableChange for adding a column.
   * <p>
   * If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the new column
   * @param dataType the new column's data type
   * @param isNullable whether the new column can contain null
   * @param comment the new field's comment string
   * @return a TableChange for the addition
   */
  static TableChange addColumn(
      String[] fieldNames,
      DataType dataType,
      boolean isNullable,
      String comment) {
    return new AddColumn(fieldNames, dataType, isNullable, comment);
  }

  /**
   * Create a TableChange for renaming a field.
   * <p>
   * The name is used to find the field to rename. The new name will replace the leaf field name.
   * For example, renameColumn(["a", "b", "c"], "x") should produce column a.b.x.
   * <p>
   * If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames the current field names
   * @param newName the new name
   * @return a TableChange for the rename
   */
  static TableChange renameColumn(String[] fieldNames, String newName) {
    return new RenameColumn(fieldNames, newName);
  }

  /**
   * Create a TableChange for updating the type of a field that is nullable.
   * <p>
   * The field names are used to find the field to update.
   * <p>
   * If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the column to update
   * @param newDataType the new data type
   * @return a TableChange for the update
   */
  static TableChange updateColumnType(String[] fieldNames, DataType newDataType) {
    return new UpdateColumnType(fieldNames, newDataType, true);
  }

  /**
   * Create a TableChange for updating the type of a field.
   * <p>
   * The field names are used to find the field to update.
   * <p>
   * If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the column to update
   * @param newDataType the new data type
   * @return a TableChange for the update
   */
  static TableChange updateColumnType(
      String[] fieldNames,
      DataType newDataType,
      boolean isNullable) {
    return new UpdateColumnType(fieldNames, newDataType, isNullable);
  }

  /**
   * Create a TableChange for updating the comment of a field.
   * <p>
   * The name is used to find the field to update.
   * <p>
   * If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the column to update
   * @param newComment the new comment
   * @return a TableChange for the update
   */
  static TableChange updateColumnComment(String[] fieldNames, String newComment) {
    return new UpdateColumnComment(fieldNames, newComment);
  }

  /**
   * Create a TableChange for deleting a field.
   * <p>
   * If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the column to delete
   * @return a TableChange for the delete
   */
  static TableChange deleteColumn(String[] fieldNames) {
    return new DeleteColumn(fieldNames);
  }

  /**
   * A TableChange to set a table property.
   * <p>
   * If the property already exists, it must be replaced with the new value.
   */
  final class SetProperty implements TableChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    public String property() {
      return property;
    }

    public String value() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SetProperty that = (SetProperty) o;
      return property.equals(that.property) &&
        value.equals(that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }
  }

  /**
   * A TableChange to remove a table property.
   * <p>
   * If the property does not exist, the change should succeed.
   */
  final class RemoveProperty implements TableChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    public String property() {
      return property;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoveProperty that = (RemoveProperty) o;
      return property.equals(that.property);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property);
    }
  }

  interface ColumnChange extends TableChange {
    String[] fieldNames();
  }

  /**
   * A TableChange to add a field.
   * <p>
   * If the field already exists, the change must result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change must
   * result in an {@link IllegalArgumentException}.
   */
  final class AddColumn implements ColumnChange {
    private final String[] fieldNames;
    private final DataType dataType;
    private final boolean isNullable;
    private final String comment;

    private AddColumn(String[] fieldNames, DataType dataType, boolean isNullable, String comment) {
      this.fieldNames = fieldNames;
      this.dataType = dataType;
      this.isNullable = isNullable;
      this.comment = comment;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }

    public DataType dataType() {
      return dataType;
    }

    public boolean isNullable() {
      return isNullable;
    }

    public String comment() {
      return comment;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AddColumn addColumn = (AddColumn) o;
      return isNullable == addColumn.isNullable &&
        Arrays.equals(fieldNames, addColumn.fieldNames) &&
        dataType.equals(addColumn.dataType) &&
        comment.equals(addColumn.comment);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(dataType, isNullable, comment);
      result = 31 * result + Arrays.hashCode(fieldNames);
      return result;
    }
  }

  /**
   * A TableChange to rename a field.
   * <p>
   * The name is used to find the field to rename. The new name will replace the leaf field name.
   * For example, renameColumn("a.b.c", "x") should produce column a.b.x.
   * <p>
   * If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  final class RenameColumn implements ColumnChange {
    private final String[] fieldNames;
    private final String newName;

    private RenameColumn(String[] fieldNames, String newName) {
      this.fieldNames = fieldNames;
      this.newName = newName;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }

    public String newName() {
      return newName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RenameColumn that = (RenameColumn) o;
      return Arrays.equals(fieldNames, that.fieldNames) &&
        newName.equals(that.newName);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(newName);
      result = 31 * result + Arrays.hashCode(fieldNames);
      return result;
    }
  }

  /**
   * A TableChange to update the type of a field.
   * <p>
   * The field names are used to find the field to update.
   * <p>
   * If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  final class UpdateColumnType implements ColumnChange {
    private final String[] fieldNames;
    private final DataType newDataType;
    private final boolean isNullable;

    private UpdateColumnType(String[] fieldNames, DataType newDataType, boolean isNullable) {
      this.fieldNames = fieldNames;
      this.newDataType = newDataType;
      this.isNullable = isNullable;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }

    public DataType newDataType() {
      return newDataType;
    }

    public boolean isNullable() {
      return isNullable;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateColumnType that = (UpdateColumnType) o;
      return isNullable == that.isNullable &&
        Arrays.equals(fieldNames, that.fieldNames) &&
        newDataType.equals(that.newDataType);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(newDataType, isNullable);
      result = 31 * result + Arrays.hashCode(fieldNames);
      return result;
    }
  }

  /**
   * A TableChange to update the comment of a field.
   * <p>
   * The field names are used to find the field to update.
   * <p>
   * If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  final class UpdateColumnComment implements ColumnChange {
    private final String[] fieldNames;
    private final String newComment;

    private UpdateColumnComment(String[] fieldNames, String newComment) {
      this.fieldNames = fieldNames;
      this.newComment = newComment;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }

    public String newComment() {
      return newComment;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateColumnComment that = (UpdateColumnComment) o;
      return Arrays.equals(fieldNames, that.fieldNames) &&
        newComment.equals(that.newComment);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(newComment);
      result = 31 * result + Arrays.hashCode(fieldNames);
      return result;
    }
  }

  /**
   * A TableChange to delete a field.
   * <p>
   * If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  final class DeleteColumn implements ColumnChange {
    private final String[] fieldNames;

    private DeleteColumn(String[] fieldNames) {
      this.fieldNames = fieldNames;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DeleteColumn that = (DeleteColumn) o;
      return Arrays.equals(fieldNames, that.fieldNames);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(fieldNames);
    }
  }

}
