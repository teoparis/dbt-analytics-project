/*
  cents_to_euros(column_expression)

  Converts an integer cent amount to a decimal EUR value.
  Always returns a numeric(18, 2) to ensure consistent precision across adapters.

  Usage in SQL:
    {{ cents_to_euros('unit_price_cents') }}           -- simple column
    {{ cents_to_euros('quantity * unit_price_cents') }} -- expression

  Note: pass expressions as strings (no column quoting inside the macro).
  The caller is responsible for ensuring the expression evaluates to a number.
*/

{% macro cents_to_euros(column_expression) %}
    cast(
        cast({{ column_expression }} as numeric(18, 4)) / 100.0
        as numeric(18, 2)
    )
{% endmacro %}
