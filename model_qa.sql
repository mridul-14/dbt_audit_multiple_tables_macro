{{ config(
    alias='audit_table',
    materialized ='table'
) }}

{%- set envs = ['<schema 1>', '<schema 2>'] -%} -- this is the schema/dataset name where the tables exist

{%- set tables = ['<table name>'] -%} -- this is the table name which you want to compare

{%- set metrics = ['<metric 1>','<metric 2>'] -%} -- this is a list of all fields whose values are to be compared

{%- set dimensions = ["<dim 1>", "<dim 2>", "<dim 3>"] -%} -- list of all dims that are to be compared, the data gets rolled over on these dimensional fields

{%- set dimensions_count = ["<dim 1>"] -%} -- list of dims whose counts you want to see (it will give distinct and null counts for values in those dimensions)

{%- set filters = ["date >= '2024-01-01'", "date <= '2024-04-30'"] -%} -- any filter that you want to apply to your data in those tables; this field should ideally be common in both tables (same name, serves the same purpose)

{%- set audit_query = audit_multiple_tables(envs, tables, metrics, dimensions, dimensions_count, filters) -%}

{{ audit_query }}
