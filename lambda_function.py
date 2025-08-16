import json
import boto3
import csv
import io
import os
from datetime import datetime

s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')

def lambda_handler(event, context):
    bucket = os.environ.get('BUCKET_NAME')
    key = os.environ.get('CSV_KEY', 'sample-sales.csv')
    model_id = os.environ.get('MODEL_ID')
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        
        reader = csv.DictReader(io.StringIO(csv_content))
        total_sales = 0
        category_sales = {}
        product_sales = {}
        
        for row in reader:
            amount = float(row['amount'])
            total_sales += amount
            category = row['category']
            category_sales.setdefault(category, 0)
            category_sales[category] += amount
            product = row['product']
            product_sales.setdefault(product, 0)
            product_sales[product] += amount
        
        top_products = sorted(product_sales.items(), key=lambda x: x[1], reverse=True)[:3]
        
        analysis_result = {
            "total_sales": total_sales,
            "category_sales": category_sales,
            "top_3_products": [{"product": p[0], "amount": p[1]} for p in top_products]
        }
        
        prompt = f"""以下の売上データを簡潔に要約してください（100文字以内）:
        総売上: {total_sales:,.0f}円
        カテゴリ別: {json.dumps(category_sales, ensure_ascii=False)}
        トップ3商品: {[p[0] for p in top_products]}
        """
        
        # Claude 3 用のリクエスト形式
        bedrock_request = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 200,
            "messages": [{"role": "user", "content": prompt}]
        }
        
        bedrock_response = bedrock.invoke_model(
            modelId=model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(bedrock_request)
        )
        
        # Claude 3 用のレスポンス形式
        response_body = json.loads(bedrock_response['body'].read())
        summary = response_body.get('content', [{}])[0].get('text', 'No summary generated')
        
        print(json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "event": "analysis_complete",
            "total_sales": total_sales
        }))
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'analysis': analysis_result,
                'summary': summary,
                'timestamp': datetime.utcnow().isoformat()
            }, ensure_ascii=False)
        }
        
    except Exception as e:
        print(json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "event": "error",
            "error": str(e)
        }))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
