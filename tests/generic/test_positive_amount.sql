/*
  Generic test: test_positive_amount

  Asserts that all non-null values in a numeric column are strictly greater than 0.
  Null values are ignored (use a separate not_null test if nulls are also invalid).

  Usage in schema.yml:
    columns:
      - name: unit_price_cents
        tests:
          - test_positive_amount:
              column_name: unit_price_cents

  The test fails (returns rows) if any non-null value is <= 0.
*/

{% test test_positive_amount(model, column_name) %}

select
    {{ column_name }} as failing_value,
    count(*) as row_count

from {{ model }}

where
    {{ column_name }} is not null
    and {{ column_name }} <= 0

group by 1

{% endtest %}
