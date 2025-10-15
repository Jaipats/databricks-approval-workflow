
```
databricks sync --watch . \
    /Workspace/Users/antoine.amend@databricks.com/capmarket-data-app/app \
    --profile DEMO

databricks apps deploy \
    capmarket-data-app \
    --source-code-path /Workspace/Users/antoine.amend@databricks.com/capmarket-data-app/app \
    --profile DEMO
```