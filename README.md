# dbt_audit_multiple_tables_macro
This is a macro to verify data between two or more tables. The user can compare the fields among many tables, this makes the QA process easier,.

Steps to use this macro -
1. Put the macro in your dbt_project folder inside the macros folder.
2. Use the model and add inputs for your schemas/dataset, tables, metrics, dimensions and filters.
3. Run the model, it will create a table with the alias name mentioned in your DB.

You can control the output schema/dataset using dbt_project.yml or the model itself or the generate_schema_name macro.
