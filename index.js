const express = require("express");

const app = express();
const amqp = require("amqplib");

require("dotenv").config();

const amqp_url = process.env.AMQP_URL;

const QUEUE_NAME = "Queue";

const port = 3000;

// Middleware
app.use(express.json());

// Giả lập cơ sở dữ liệu trong bộ nhớ
const buyOrders = [];
const sellOrders = [];

// API để gửi lệnh mua
app.post("/buy", async (req, res) => {
  const { code, price, quantity, user } = req.body;
  if (!code || !price || !quantity || !user) {
    return res.status(400).send("Missing required fields");
  }

  const order = { code, price, quantity, user, type: "buy" };
  await sendQueue(order);
  res.status(201).send(order);
});

// API để gửi lệnh bán
app.post("/sell", async (req, res) => {
  const { code, price, quantity,user } = req.body;
  if (!code || !price || !quantity) {
    return res.status(400).send("Missing required fields");
  }
  const order = { code, price, quantity, type: "sell", user };
  await sendQueue(order);
  res.status(201).send(order);
});

app.get("/list/buy", async (req, res) => {
    res.status(200).json(buyOrders);
})

app.get("/list/sell", async (req, res) => {
    res.status(200).json(sellOrders);
})

const sendQueue = async (info) => {
  const connect = await amqp.connect(amqp_url);
  const channel = await connect.createChannel();
  await channel.assertQueue(QUEUE_NAME, {
    durable: false,
  });
  await channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(info)));
  console.log("send: ", info);
};

const receiveQueue = async () => {
  const connect = await amqp.connect(amqp_url);
  const channel = await connect.createChannel();
  await channel.assertQueue(QUEUE_NAME, {
    durable: false,
  });
  await channel.consume(
    QUEUE_NAME,
    (msg) => {
      const order = JSON.parse(msg.content.toString());
      if (order.type === "buy") {
        buyOrders.push(order);
        console.log(
          `User ${order.user} buys: Code ${order.code} - Price: ${order.price} -  Quantity ${order.quantity}.`
        );
      } else if (order.type === "sell") {
        sellOrders.push(order);
        console.log(
          `User ${order.user} sells: Code ${order.code} - Price: ${order.price} -  Quantity ${order.quantity}.`
        );
      }
      matchOrders();
    },
    {
      noAck: true,
    }
  );
};

function matchOrders() {
  buyOrders.forEach((buyOrder, buyIndex) => {
    sellOrders.forEach((sellOrder, sellIndex) => {
      // Kiểm tra điều kiện khớp lệnh
      if (
        buyOrder.code === sellOrder.code &&
        buyOrder.price === sellOrder.price
      ) {
        console.log(
          `Tìm thấy lệnh khớp: ${buyOrder.code} với giá ${buyOrder.price}`
        );

        // Xác định số lượng có thể khớp
        const matchedQuantity = Math.min(
          buyOrder.quantity,
          sellOrder.quantity
        );

        // Cập nhật quantity cho cả hai lệnh
        buyOrder.quantity -= matchedQuantity;
        sellOrder.quantity -= matchedQuantity;

        console.log(
          `Khớp ${matchedQuantity} cổ phiếu. Còn lại: Mua ${buyOrder.quantity}, Bán ${sellOrder.quantity}`
        );

        // Kiểm tra và xử lý nếu quantity của lệnh mua = 0
        if (buyOrder.quantity === 0) {
          console.log(
            `Lệnh mua đã được hoàn tất và sẽ được xóa. Mã lệnh mua: ${buyOrder.user}-${buyOrder.code}`
          );
          buyOrders.splice(buyIndex, 1); // Xóa lệnh mua khỏi mảng
        }

        // Kiểm tra và xử lý nếu quantity của lệnh bán = 0
        if (sellOrder.quantity === 0) {
          console.log(
            `Lệnh bán đã được hoàn tất và sẽ được xóa. Mã lệnh bán: ${sellOrder.user}-${sellOrder.code}`
          );
          sellOrders.splice(sellIndex, 1); // Xóa lệnh bán khỏi mảng
        }
      }
    });
  });
}

async function run() {
  await receiveQueue();

  app.listen(port, () => {
    console.log(`Stock trading app listening at http://localhost:${port}`);
  });
}

run();
