package com.selectdb.dynamic;

public class FieldSchema {
    private String name;
    private String typeString;
    private String defaultValue;
    private String comment;

    public FieldSchema() {}

    public FieldSchema(String name, String typeString, String comment) {
        this.name = name;
        this.typeString = typeString;
        this.comment = comment;
    }

    public FieldSchema(String name, String typeString, String defaultValue, String comment) {
        this.name = name;
        this.typeString = typeString;
        this.defaultValue = defaultValue;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTypeString() {
        return typeString;
    }

    public void setTypeString(String typeString) {
        this.typeString = typeString;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public String toString() {
        return "FieldSchema{"
                + "name='"
                + name
                + '\''
                + ", typeString='"
                + typeString
                + '\''
                + ", defaultValue='"
                + defaultValue
                + '\''
                + ", comment='"
                + comment
                + '\''
                + '}';
    }
}
