#  Config management.

Included Config file which makes code reusable. For transformation changes we don't have to change application.

read_file_format  - We can change it to JSON or CSV

input_header - Header of  input file to test

output_header - Header of  output file to test

final_sql - transformation which need to be writen in output file.

## Logging and alerting.

Added exception handling to every function so error resolution can be eased. Also added tests for easy diagnosis and resolution of errors


##  Data quality checks (like input/output dataset validation).

Included file format check before reading file.

Input file Header Check with prespecified Header values.

output file Header Check with prespecified Header values.

## Implement CI/CD
Created application with segregated modules  which eases maintainability and Each function can be changed and integrated. Also every function can be tested separately.

## Diagnose and tune the application

  We can calculate executers/cores as per data requirements of application.

  Included exceptions and exists for easy diagnose and resolution of errors.

  We can also test with load to analyze which module is taking longer time. That can help in diagnosis of performance issues.

## Schedule this pipeline 
  we can schedule application with Autosys/control_m/Airflow.
