#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Reset Data"
echo "====================================="

rm -f data/raw/Questions.csv
rm -f data/raw/Answers.csv
rm -rf data/output/question_answer_count

echo "Local data cleaned:"
echo "- data/raw/Questions.csv"
echo "- data/raw/Answers.csv"
echo "- data/output/question_answer_count"

echo ""
echo "If you want to also reset MongoDB collections, run:"
echo "docker exec -it asm2-mongo mongosh"
echo ""
echo "Then inside mongosh:"
echo "use stackoverflow_asm2"
echo "db.questions.drop()"
echo "db.answers.drop()"
echo "db.answer_counts.drop()"