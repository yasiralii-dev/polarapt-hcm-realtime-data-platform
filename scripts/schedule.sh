#!/bin/bash
# Usage: ./scripts/schedule.sh <lambda_name> <rule_name> [schedule_expression] [ENABLED|DISABLED]
set -e
FN=$1
RULE=$2
SCHED=${3:-"rate(1 minute)"}
STATE=${4:-ENABLED}

aws events put-rule --name "$RULE" --schedule-expression "$SCHED" --state "$STATE" >/dev/null

FN_ARN=$(aws lambda get-function --function-name "$FN" --query Configuration.FunctionArn --output text)
RULE_ARN=$(aws events describe-rule --name "$RULE" --query Arn --output text)

aws lambda add-permission --function-name "$FN" --statement-id "eventbridge-$RULE" \
  --action lambda:InvokeFunction --principal events.amazonaws.com \
  --source-arn "$RULE_ARN" >/dev/null 2>&1 || true

aws events put-targets --rule "$RULE" --targets "Id=1,Arn=$FN_ARN" >/dev/null
echo "Rule $RULE -> $FN | $SCHED | $STATE"
