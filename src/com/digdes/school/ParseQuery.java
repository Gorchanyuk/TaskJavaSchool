package com.digdes.school;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ParseQuery {

    private final String regex =
            "(?i)^\\s*(INSERT|UPDATE|SELECT|DELETE)\\s*(?:(?:VALUES)\\s*(.*?))?\\s*(?:(?:WHERE\\s*)(.+))?$";
    private final String valueRegex =
            "(?i)(.*?)?\\s*('(\\w+)'\\s*=\\s*(-?\\d+(?:\\.\\d+)?|true|false|null|'(?:[^']*?)'))?\\s*(?:,|$)";
    private final String whereRegex =
            "(?i)(.*?)?\\s*(AND|OR)?\\s*('(\\w+)'\\s*(=|!=|>|<|>=|<=|LIKE|ILIKE)\\s*(-?\\d+(?:\\.\\d+)?|true|false|null|'(?:[^']*?)'))?(\\s+|$)";
    private String typeOperation;
    private Boolean valueExist;
    private String values;
    private Boolean whereExist;
    private String condition;

    public ParseQuery(String query) throws Exception {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(query);
        if (matcher.find()) {
            typeOperation = matcher.group(1);//Тип операции, может быть INSERT, UPDATE, SELECT или DELETE
            values = matcher.group(2);//Строка со начениями в поле VALUE
            condition = matcher.group(3);//Строка с с условиями в поле WHERE
            valueExist = values != null && !values.isEmpty();//Присутствует ли поле VALUES в запросе
            whereExist = condition != null;//Присутствует ли поле WHERE в запросе
        }
        if (typeOperation == null) {
            throw new Exception("Некорректный запрос");
        }
    }

    public String getTypeOperation() {
        return typeOperation;
    }

    public Boolean isValueExist() {
        return valueExist;
    }

    public Boolean isWhereExist() {
        return whereExist;
    }

    public Map<String, Object> getValues() throws Exception {
        //Возвращает Map содержащую значения переданные в запросе в поле VALUE
        Map<String, Object> res = new HashMap<>();
        Pattern pattern = Pattern.compile(valueRegex);
        Matcher matcher = pattern.matcher(values);
        while (matcher.find()) {
            String somethingIsNotValid = matcher.group(1);//Блок запроса не соответствующий шаблону
            if (somethingIsNotValid.length() > 0)
                throw new Exception("Ошибка в запросе, примерное место - " + somethingIsNotValid);
            if (matcher.group(2) == null)
                //Если строка с условиями не содержит ошибок, то эта проверка позволит выйти из цикла
                //Нужно для корректной проверки последней пары ключ - значение
                continue;
            String key = matcher.group(3).toUpperCase();
            String value = matcher.group(4);
            if (!isTypesEquals(key, value))
                throw new Exception("Переданный тип данных, для поля " + key + " не соответствует ожидаемому");
            res.putAll(getValidatedValues(key, value, true));
        }
        return res;
    }

    public List<Map<String, Object>> getListMatchingConditions() throws Exception {
        String[] arrConditions = condition.split("(?i)\\s(OR)\\s");
        List<Map<String, Object>> resultCondition = new ArrayList<>();
        List<Map<String, Object>> temp;
        for (String subcondition : arrConditions) {
            temp = getListMatchingSubconditions(subcondition);
            //Ниже проверка делается на тот случай если в БД содержится несколько полностью идентичных строк
            for (Map<String, Object> row : temp) {
                //Для каждой записи из найденного списка проверяем не содержит ли наш ответ уже такую запись
                //если содержит, проверяем именно эта запись в запросе или просто с такими же значениями,
                //если запись таже то удаляем ее из нашего результата, чтобы не было дубликатов
                if (resultCondition.contains(row)) {
                    resultCondition = resultCondition.stream()
                            .filter(map -> map != row)//проверка по ссылке
                            .collect(Collectors.toList());
                }
            }
            resultCondition.addAll(temp);
        }
        return resultCondition;
    }

    private List<Map<String, Object>> getListMatchingSubconditions(String subcondition) throws Exception {
        Pattern wherePattern = Pattern.compile(whereRegex);
        Matcher whereMatcher = wherePattern.matcher(subcondition);
        List<Map<String, Object>> resultCondition = new ArrayList<>();
        while (whereMatcher.find()) {
            String somethingIsNotValid = whereMatcher.group(1);
            if (somethingIsNotValid.length() > 0)
                throw new Exception("Ошибка в запросе, примерное место - " + subcondition);
            if (whereMatcher.group(3) == null) {
                //Нужно для корректной проверки последней пары ключ - значение
                continue;
            }
            String logicalOperator = whereMatcher.group(2);
            String key = whereMatcher.group(4).toUpperCase();
            String operator = whereMatcher.group(5).toUpperCase();
            String value = whereMatcher.group(6);

            if (!isTypeCanUseOperator(key, operator))
                throw new Exception(
                        "Данный оператор сравнения '" + operator + "' нельзя использовать с типом данных который хранится в колонке '" + key + "'");
            if (logicalOperator == null) {
                //Ищем соответствия первому условию из строки subcondition
                resultCondition.addAll(getListMatchOneCondition(key, operator, value));
            } else
                // В уловии используется логическое И,
                // Удаляем из пердыдущих поисков строки которые не соответствуют этому условию
                resultCondition.retainAll(getListMatchOneCondition(key, operator, value));
        }
        return new ArrayList<>(resultCondition);
    }

    private List<Map<String, Object>> getListMatchOneCondition(String key, String operator, String value)
            throws Exception {
        // Получаем список соответствующий одному, переданному условию

        Map<String, Object> map = getValidatedValues(key, value, false);
        List<Map<String, Object>> result = new ArrayList<>();
        key = ColumnName.valueOf(key).getName();
        for (Map<String, Object> row : DataBase.getTable()) {
            Object currentValue = row.get(key);
            if (currentValue == null)
                continue;
            switch (operator) {
                //Проверка всех возможных условий и добавление соответствующей строки из БД во временный список
                case "=":
                    if (currentValue.equals(map.get(key))) {
                        result.add(row);
                    }
                    break;
                case "!=":
                    if (!currentValue.equals(map.get(key))) {
                        result.add(row);
                    }
                    break;
                case ">":
                    assertIsNumber(map.get(key));
                    if (((Number) currentValue).doubleValue() > ((Number) map.get(key)).doubleValue()) {
                        result.add(row);
                    }
                    break;
                case "<":
                    assertIsNumber(map.get(key));
                    if (((Number) currentValue).doubleValue() < ((Number) map.get(key)).doubleValue()) {
                        result.add(row);
                    }
                    break;
                case ">=":
                    assertIsNumber(map.get(key));
                    if (((Number) currentValue).doubleValue() >= ((Number) map.get(key)).doubleValue()) {
                        result.add(row);
                    }
                    break;
                case "<=":
                    assertIsNumber(map.get(key));
                    if (((Number) currentValue).doubleValue() <= ((Number) map.get(key)).doubleValue()) {
                        result.add(row);
                    }
                    break;
                case "LIKE":
                    assertIsString(map.get(key));
                    if (currentValue.toString().matches(map.get(key).toString().replace("%", ".*"))) {
                        result.add(row);
                    }
                    break;
                case "ILIKE":
                    assertIsString(map.get(key));
                    if (currentValue.toString().toLowerCase().matches(map.get(key).toString().toLowerCase().replace("%", ".*"))) {
                        result.add(row);
                    }
                    break;
            }
        }
        return result;
    }

    private void assertIsNumber(Object value) throws Exception {
        //Используется в операторах сравнения
        if (!(value instanceof Number)) {
            throw new Exception("Некорректный тип данных в условии");
        }
    }

    private void assertIsString(Object value) throws Exception {
        //Используется в операторах сравнения
        if (!(value instanceof String))
            throw new Exception("Некорректный тип данных в условии");
    }

    private Map<String, Object> getValidatedValues(String key, String value, Boolean valuesCanBeNull)
            throws Exception {
//        Проверяем соответствие полученных значений ожидаемым.
//        valuesCanBeNull должно быть true если проверяем значения для записи в БД,
//        и false если проверяем значения из условия
        Map<String, Object> res = new HashMap<>();
        String currentType;
        try {
            ColumnName column = ColumnName.valueOf(key);
            if (valuesCanBeNull) {
                //если valuesCanBeNull = true, берем значение которое должно храниться в БД
                currentType = column.getType();
            } else {
                //Если valuesCanBeNull = false, получаем актуальный тип полученных данных
                currentType = getTypeValueFromRequest(value);
                if(currentType == null)
                    throw new Exception("Значения которые передаются на сравнение не могут быть null");
            }
            if (currentType.equals("String") && !value.equalsIgnoreCase("null")) {
                // Если тип значения это строка и не равна null"
                // Убираем одинарные ковычки и перезаписываем значение value
                value = value.substring(1, value.length() - 1);
            }

            Class clazz = Class.forName("java.lang." + currentType);
            if (value.equalsIgnoreCase("null") && valuesCanBeNull) {
                res.put(column.getName(), null);
            } else if (clazz.equals(Boolean.class)) {
//                Значения которые передаются в колонку со значением bool могут равняться только true или false
                if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                    throw new Exception("Некорректно указанно значение, тип данных не соответствует ожидаемому.");
                }
                res.put(column.getName(), Boolean.parseBoolean(value));
            } else if (!clazz.equals(String.class)) {
                //Проверка валидности значений передаваемых в колонки с типом Long и Double
                Object val = clazz.getMethod("parse" + currentType, String.class)
                        .invoke(clazz, value);
                res.put(column.getName(), val);
            } else {
                //если дошли до сюда значит колонка имеет тип String, записываем значение пришедшее в метод
                res.put(column.getName(), value);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return res;
    }

    private String getTypeValueFromRequest(String value) throws Exception {
        if (value.matches("^'.*'$")) {
            return "String";
        } else if (value.toLowerCase().matches("(true|false)")) {
            return "Boolean";
        } else if (value.matches("-?\\d+(?:\\.\\d+)"))
            return "Double";
        else if (value.matches("-?\\d+"))
            return "Long";
        else if (value.toLowerCase().matches("null"))
            return null;
        else
            throw new Exception("Недопустимый формат значения " + value);
    }

    private boolean isTypesEquals(String key, String value) throws Exception {
        String currentType = getCurrentTypeFromKey(key);
        String requestType = getTypeValueFromRequest(value);
        //Ожидаемый тип данных и переданный для сравнения должны быть идентичны
        if (currentType.equals("Double") && requestType.equals("Long")) {
            return true;
        } else if (requestType == null)
            return true;
        else
            return currentType.equals(requestType);
    }

    private boolean isTypeCanUseOperator(String key, String operator) throws Exception {
        String currentType = getCurrentTypeFromKey(key);
        if (operator.equals("=") || operator.equals("!="))
            //Все типы могут использовать операторы '=' и '!='
            return true;
        else if (currentType.equals("String") && (operator.equals("LIKE") || operator.equals("ILIKE")))
            //Только строки могут использовать операторы 'like' и 'ilike'
            return true;
        else
            //Типы 'Long' и 'Double' могут быть использованны со всеми операторами кроме 'like' и 'ilike'
            return (currentType.equals("Long") || currentType.equals("Double"))
                    && !operator.equals("LIKE") && !operator.equals("ILIKE");
    }

    private String getCurrentTypeFromKey(String key) throws Exception {
        String currentType;
        try {
            currentType = ColumnName.valueOf(key).getType();
        } catch (IllegalArgumentException e) {
            throw new Exception("Колонки с названием '" + key + "' не существует");
        }
        return currentType;
    }
}