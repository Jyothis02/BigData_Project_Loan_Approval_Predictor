#!/usr/bin/env python3

from flask import Flask, jsonify
from pymongo import MongoClient
from datetime import datetime, timedelta
import json

app = Flask(__name__)

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['loan_db']

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get overall statistics"""
    try:
        total_apps = db.processed_applications.count_documents({})
        predictions = db.predictions.find()
        pred_list = list(predictions)
        
        if pred_list:
            approved = sum(1 for p in pred_list if p.get('prediction') == 1)
            approval_rate = (approved / len(pred_list)) * 100
            avg_confidence = sum(p.get('confidence', 0) for p in pred_list) / len(pred_list)
        else:
            approval_rate = 0
            avg_confidence = 0
            approved = 0
        
        return jsonify({
            'total_applications': total_apps,
            'total_predictions': len(pred_list),
            'approved_count': approved,
            'denied_count': len(pred_list) - approved,
            'approval_rate': round(approval_rate, 2),
            'avg_confidence': round(avg_confidence, 4),
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/approval-trend', methods=['GET'])
def approval_trend():
    """Get approval rate over time (last 24 hours)"""
    try:
        pipeline = [
            {
                '$group': {
                    '_id': {
                        '$dateToString': {
                            'format': '%Y-%m-%d %H:00',
                            'date': {'$toDate': {'$multiply': ['$timestamp', 1000]}}
                        }
                    },
                    'approved': {
                        '$sum': {'$cond': [{'$eq': ['$prediction', 1]}, 1, 0]}
                    },
                    'denied': {
                        '$sum': {'$cond': [{'$eq': ['$prediction', 0]}, 1, 0]}
                    }
                }
            },
            {'$sort': {'_id': 1}},
            {'$limit': 24}
        ]
        
        results = list(db.predictions.aggregate(pipeline))
        
        return jsonify({
            'data': results,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/confidence-distribution', methods=['GET'])
def confidence_distribution():
    """Get distribution of prediction confidence scores"""
    try:
        pipeline = [
            {
                '$bucket': {
                    'groupBy': '$confidence',
                    'boundaries': [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
                    'default': 'other',
                    'output': {
                        'count': {'$sum': 1}
                    }
                }
            }
        ]
        
        results = list(db.predictions.aggregate(pipeline))
        
        return jsonify({
            'data': results,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route("/api/model-metrics", methods=["GET"])
def get_model_metrics():
    metrics = list(db.model_metrics.find({}, {"_id": 0}))
    return jsonify(metrics)

@app.route('/api/health', methods=['GET'])
def health_check():
    """Check if MongoDB is connected"""
    try:
        db.command('ping')
        return jsonify({'status': 'healthy', 'mongodb': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

@app.route('/')
def root():
    return jsonify({'status': 'ok', 'message': 'Loan Prediction API'})

if __name__ == '__main__':
    print("Starting Grafana API server...")
    print("Available endpoints:")
    print("  - http://localhost:5000/api/stats")
    print("  - http://localhost:5000/api/approval-trend")
    print("  - http://localhost:5000/api/confidence-distribution")
    print("  - http://localhost:5000/api/health")
    print("")
    app.run(debug=True, host='0.0.0.0', port=5000)
