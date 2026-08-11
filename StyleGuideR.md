### General rules for writing R code

- Do not use `<-` fo r assignment, use `=` instead
- Use `%>%` pipe operator
- Use `snake_case` for variable and function names
- Be sparingly with line breaks. Within a statement, line breaks should only be 
  introduced in the following cases:
  - to keep lines under 80 characters
  - after the `%>%` operator when using the tidyverse
  - after the `+` operator when building ggplot objects
  - for a new item within the `ggplot2::theme` function
  - after and before curly braces `{}` when defining functions and loops
  - When breaking for 80-char compliance, content must start on the same line as the opening expression
- Lines should never end with `(` or start with `)`
- Whenever possible, use functions from the `tidyverse` collection of packages
- Use comments very sparingly. The ideal code does not need any comments, but it self-documenting through variable, data set and function names
