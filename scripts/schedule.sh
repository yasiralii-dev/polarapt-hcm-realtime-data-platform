#!/bin/bash
# Usage: ./scripts/schedule.sh <lambda_function_name> <rule_name>
set -e
FN=$1
RULE=$2

aws events put-rule --name "$RULE" --schedule-expression "rate(1 minute)" --state ENABLED

FN_ARN=$(aws lambda get-function --function-name "$FN" --query Configuration.FunctionArn --output text)
RULE_ARN=$(aws events describe-rule --name "$RULE" --query Arn --output text)

aws lambda add-permission \
  --function-name "$FN" \
  --statement-id "eventbridge-$RULE" \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "$RULE_ARN"

aws events put-targets --rule "$RULE" --targets "Id=1,Arn=$FN_ARN"
echo "Scheduled $FN every minute via rule $RULE"
