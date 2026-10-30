<p align="center"><img src="branding/logo.svg" alt="swarmqtt" width="360"></p>

<p align="center"><b>Unleash a swarm of IoT devices on your MQTT broker.</b></p>

# swarmqtt

Web UI สำหรับ load test MQTT / MQTTS broker จำลอง IoT device ได้เป็นแสนตัว
ฝั่ง load ใช้ [emqtt-bench](https://github.com/emqx/emqtt-bench) และฝั่ง UI แตก worker container ให้อัตโนมัติ

## ใช้งาน

```sh
docker compose up -d --build          # เปิด http://localhost:8080
UI_PORT=18080 docker compose up -d    # ถ้า port 8080 ไม่ว่าง
docker compose down                   # ปิด UI (ให้กด Stop ก่อนเพื่อลบ worker)
```

1. กรอก Broker (host, port, MQTT version, user/pass) ถ้า broker รันบนเครื่องนี้ ให้ใช้ `host.docker.internal`
2. TLS: เปิด MQTTS, วางหรืออัปโหลด CA / client cert / client key (PEM)
3. กด **Test connection**: ต่อ 1 connection ด้วย paho-mqtt และ verify cert จริงถ้าติ๊ก Verify
4. ตั้ง **Concurrent devices**, connections/sec, mode (publish / subscribe / connect only), topic, ความถี่, payload, QoS
5. กด **Start load test** แล้วดู connected / failed / rate / log สดๆ ส่วน **Stop** ลบ worker ทั้งหมด

แต่ละ worker container รับไม่เกิน 25,000 device (มี IP ของตัวเอง ไม่ติด limit ~64k source port ต่อ IP)
เช่น 100,000 device = 4 worker

## ข้อควรรู้

- **ต้องเตรียม RAM เยอะ**: วัดได้ว่า worker 1 ตัวใช้ ~575 MB บวก ~107 KB ต่อ TLS connection
  100k device ≈ 13 GB ฝั่ง bench อย่างเดียว (ถ้าใช้ Docker Desktop/colima ต้องเพิ่ม memory ของ VM)
  ถ้าเกินเครื่องเดียว ให้รัน UI นี้หลายเครื่อง แต่ละเครื่องรับส่วนหนึ่ง และตั้ง Client ID prefix ไม่ให้ซ้ำกัน
- **ตอน load test ไม่ verify cert ของ broker** (เป็นข้อจำกัดของ emqtt-bench) CA จึงใช้แค่ตอน Test connection
- **ตัวเลข `pub` ที่ emqtt-bench รายงานเป็น ~2 เท่าของที่ broker ได้รับจริง** ให้ดู message rate จากฝั่ง broker
- **UI นี้คุมทั้ง Docker ได้** (mount `docker.sock`) จึง bind แค่ `127.0.0.1` ห้ามเปิดออก network
- cert/key ที่ใส่จะถูกเก็บใน docker volume `swarmqtt-certs` ส่วน browser จำค่าในฟอร์มไว้ ยกเว้น password และ private key
