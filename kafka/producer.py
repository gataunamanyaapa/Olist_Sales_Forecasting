#!/usr/bin/env python3
"""
Kafka Producer: Simulasi streaming order baru Olist
Mengirim data order setiap 2 detik ke topic 'olist-orders'
Jalankan: pip install kafka-python && python3 kafka/producer.py
"""
from kafka import KafkaProducer
import json, time, random, uuid
from datetime import datetime

print("Menghubungkan ke Kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )
    print("✓ Terhubung ke Kafka!\n")
except Exception as e:
    print(f"✗ Gagal koneksi: {e}")
    exit(1)

TOPIC = "olist-orders"

STATUSES   = ["created","approved","processing","shipped","delivered"]
CITIES     = ["sao paulo","rio de janeiro","belo horizonte","salvador",
              "fortaleza","curitiba","manaus","recife","porto alegre"]
STATES     = ["SP","RJ","MG","BA","CE","PR","AM","PE","RS"]
CATEGORIES = ["electronics","fashion","home_appliances","books",
              "sports","toys","health_beauty","food_drink"]
PAYMENTS   = ["credit_card","boleto","voucher","debit_card"]

count = 0
print(f"Streaming ke topic '{TOPIC}'... (Ctrl+C untuk stop)\n")

while True:
    try:
        order_id = str(uuid.uuid4()).replace("-","")
        state_idx = random.randint(0, len(STATES)-1)

        order = {
            "order_id": order_id,
            "customer_id": str(uuid.uuid4()).replace("-",""),
            "order_status": random.choice(STATUSES),
            "customer_city": random.choice(CITIES),
            "customer_state": STATES[state_idx],
            "product_category": random.choice(CATEGORIES),
            "payment_type": random.choice(PAYMENTS),
            "payment_value": round(random.uniform(15.0, 800.0), 2),
            "freight_value": round(random.uniform(5.0, 80.0), 2),
            "review_score": random.randint(1, 5),
            "timestamp": datetime.now().isoformat()
        }

        producer.send(TOPIC, key=order_id, value=order)
        count += 1
        print(f"  [{count}] Order: {order_id[:12]}... | "
              f"Status: {order['order_status']:<12} | "
              f"Value: R${order['payment_value']:.2f} | "
              f"City: {order['customer_city']}")

        time.sleep(2)

    except KeyboardInterrupt:
        print(f"\n\nProducer dihentikan. Total terkirim: {count} orders.")
        producer.flush()
        producer.close()
        break
