package com.digdes.school;

public enum ColumnName {
    ID("id", "Long"),
    LASTNAME("lastName", "String"),
    AGE("age", "Long"),
    COST("cost", "Double"),
    ACTIVE("active", "Boolean");

    //Поле name будем использовать для хранения в БД, на тот случай если в тестах это принципиально
    private final String name;
    private final String type;


    ColumnName(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }
}
