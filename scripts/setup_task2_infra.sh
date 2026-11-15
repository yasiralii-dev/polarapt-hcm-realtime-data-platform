#!/bin/bash
set -e
: "${BUCKET:?run: source .env first}"

RAW_TOPIC_ARN=$(aws sns create-topic --name hcm-raw-events --query TopicArn --output text)
FIFO_TOPIC_ARN=$(aws sns create-topic --name hcm-routed-events.fifo \
  --attributes FifoTopic=true,ContentBasedDeduplication=false \
  --query TopicArn --output text)

for S in jobs benchmarks fx; do
  QURL=$(aws sqs create-queue --queue-name "$S-queue.fifo" \
    --attributes FifoQueue=true,VisibilityTimeout=120 \
    --query QueueUrl --output text)
  QARN=$(aws sqs get-queue-attributes --queue-url "$QURL" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)

  POLICY=$(python3 -c "import json,sys; print(json.dumps({'Version':'2012-10-17','Statement':[{'Effect':'Allow','Principal':{'Service':'sns.amazonaws.com'},'Action':'sqs:SendMessage','Resource':sys.argv[1],'Condition':{'ArnEquals':{'aws:SourceArn':sys.argv[2]}}}]}))" "$QARN" "$FIFO_TOPIC_ARN")
  python3 -c "import json,sys; print(json.dumps({'Policy':sys.argv[1]}))" "$POLICY" > /tmp/qattrs.json
  aws sqs set-queue-attributes --queue-url "$QURL" --attributes file:///tmp/qattrs.json

  python3 -c "import json,sys; print(json.dumps({'FilterPolicy':json.dumps({'source':[sys.argv[1]]}),'RawMessageDelivery':'true'}))" "$S" > /tmp/subattrs.json
  aws sns subscribe --topic-arn "$FIFO_TOPIC_ARN" --protocol sqs \
    --notification-endpoint "$QARN" --attributes file:///tmp/subattrs.json >/dev/null
  echo "OK: $S-queue.fifo subscribed with filter source=$S"
done

sed -i '/RAW_TOPIC_ARN/d;/FIFO_TOPIC_ARN/d' .env
echo "" >> .env
echo "export RAW_TOPIC_ARN=$RAW_TOPIC_ARN" >> .env
echo "export FIFO_TOPIC_ARN=$FIFO_TOPIC_ARN" >> .env
echo "RAW_TOPIC_ARN=$RAW_TOPIC_ARN"
echo "FIFO_TOPIC_ARN=$FIFO_TOPIC_ARN"
