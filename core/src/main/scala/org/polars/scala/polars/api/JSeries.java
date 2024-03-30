package org.polars.scala.polars.api;

import scala.Boolean;
import scala.Int;
import scala.jdk.javaapi.CollectionConverters;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class JSeries {
    final static String EmptyString = "";

    @SuppressWarnings({ "unchecked", "rawtypes" })
    static Series ofList(String name, Iterable<Iterable> values) {
        Iterator<Iterable> valuesIter = values.iterator();
        List<Series> sList = new ArrayList<>();

        while (valuesIter.hasNext()) {
            Iterable subList = valuesIter.next();
            Object head = subList.iterator().next();

            Series thisSeries;
            if (head instanceof Integer || head instanceof Int) {
                thisSeries = Series.ofInt(EmptyString, subList);
            } else if (head instanceof Long) {
                thisSeries = Series.ofLong(EmptyString, subList);
            } else if (head instanceof Float) {
                thisSeries = Series.ofFloat(EmptyString, subList);
            } else if (head instanceof Double) {
                thisSeries = Series.ofDouble(EmptyString, subList);
            } else if (head instanceof Boolean) {
                thisSeries = Series.ofBoolean(EmptyString, subList);
            } else if (head instanceof LocalDate) {
                thisSeries = Series.ofDate(EmptyString, subList);
            } else if (head instanceof LocalDateTime) {
                thisSeries = Series.ofDateTime(EmptyString, subList);
            } else if (head instanceof String) {
                thisSeries = Series.ofString(EmptyString, subList);
            } else if (head instanceof java.lang.Iterable) {
                thisSeries = ofList(EmptyString, subList);
            } else if (head instanceof scala.collection.Iterable) {
                Iterable<Iterable> s = (Iterable<Iterable>) StreamSupport.stream(subList.spliterator(), false)
                        .map(v -> CollectionConverters.asJava((scala.collection.Iterable) v))
                        .collect(Collectors.toList());

                thisSeries = ofList(EmptyString, s);
            } else if (head.getClass().isArray()) {
                Iterable<Iterable> s = (Iterable<Iterable>) StreamSupport.stream(subList.spliterator(), false)
                        .map(v -> Arrays.asList((Object[]) v))
                        .collect(Collectors.toList());

                thisSeries = ofList(EmptyString, s);
            } else {
                throw new IllegalArgumentException(
                        String.format("Nested series of provided internal type `%s` is currently not supported.", head.getClass().getSimpleName())
                );
            }

            sList.add(thisSeries);
        }

        return Series.ofSeries(name, sList);
    }
}
