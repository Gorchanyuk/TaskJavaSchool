package com.digdes.school;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DataBase {

    private static List<Map<String, Object>> table;

    static {
        table = new ArrayList<>();
    }

    public static List<Map<String, Object>> getTable() {
        return table;
    }

    public static List<Map<String, Object>> executeQuery(String request) {
        try {
            ParseQuery parseQuery = new ParseQuery(request);
            String typeOperation = parseQuery.getTypeOperation();

            switch (typeOperation.toUpperCase()) {
                case "INSERT":
                    return insertQuery(parseQuery);
                case "SELECT":
                    return selectQuery(parseQuery);
                case "UPDATE":
                    return updateQuery(parseQuery);
                case "DELETE":
                    return deleteQuery(parseQuery);
            }
        } catch (
                Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    private static List<Map<String, Object>> insertQuery(ParseQuery parseQuery) throws Exception {
        if (parseQuery.isWhereExist())
            throw new Exception("Запрос типа 'INSERT' не может содержать поле 'WHERE'");
        if (!parseQuery.isValueExist())
            throw new Exception("Запрос типа 'INSERT' должен содержать поле 'VALUES'");
        List<Map<String, Object>> res = new ArrayList<>();
        Map<String, Object> row = new HashMap<>(parseQuery.getValues());
        deleteFieldsWhereValueNull(row);
        res.add(row);
        if (!row.isEmpty())
            //Добавляем строку в таблицу, только в том случае, если она содержит хоть одну запись
            table.add(row);
        return res;
    }

    private static List<Map<String, Object>> selectQuery(ParseQuery parseQuery) throws Exception {
        if (parseQuery.isValueExist())
            throw new Exception("Запрос типа 'SELECT' не может содержать поле 'VALUES'");
        if (!parseQuery.isWhereExist())
            return table;
        return parseQuery.getListMatchingConditions();
    }

    private static List<Map<String, Object>> updateQuery(ParseQuery parseQuery) throws Exception {
        if (!parseQuery.isValueExist()) {
            throw new Exception("Запрос типа 'UPDATE' должен содержать поле 'VALUES'");
        } else if (!parseQuery.isWhereExist()) {
            for (Map<String, Object> row : table) {
                row.putAll(parseQuery.getValues());
                deleteFieldsWhereValueNull(row);
            }
            table.removeIf(Map::isEmpty);//Убираем пустые строки
            return table;
        }
        List<Map<String, Object>> res = parseQuery.getListMatchingConditions();
        for (Map<String, Object> row : res) {
            row.putAll(parseQuery.getValues());
            deleteFieldsWhereValueNull(row);
        }
        table.removeIf(Map::isEmpty);//Убираем пустые строки
        return res;
    }

    private static List<Map<String, Object>> deleteQuery(ParseQuery parseQuery) throws Exception {
        List<Map<String, Object>> res;
        if (parseQuery.isValueExist())
            throw new Exception("Запрос типа 'DELETE' не может содержать поле 'VALUES'");
        if (!parseQuery.isWhereExist()) {
            res = table;
            table = new ArrayList<>();
            return res;
        }
        res = parseQuery.getListMatchingConditions();
        for (Map<String, Object> row : res)
            table.remove(row);
        return res;
    }

    private static void deleteFieldsWhereValueNull(Map<String, Object> row) {
        //Удаляем из строки бд все записи которые не имеют значения
        row.entrySet().removeIf(entry -> entry.getValue() == null);
    }

}
