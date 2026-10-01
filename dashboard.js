require("dotenv").config();

const { MongoClient } = require("mongodb");

const uri = process.env.MONGODB_URI;
const dbName = process.env.MONGODB_DATABASE || "streaming_demo";

const client = new MongoClient(uri);

// --------------------------------------------------
// Helpers
// --------------------------------------------------

function toNumber(value) {
  if (value === null || value === undefined) {
    return 0;
  }

  if (typeof value === "object" && value.toString) {
    return Number(value.toString());
  }

  return Number(value);
}

function money(value) {
  return `$${toNumber(value).toFixed(2)}`;
}

function formatTime(value) {
  if (value === null || value === undefined) {
    return "N/A";
  }

  const date = new Date(toNumber(value));

  if (isNaN(date.getTime())) {
    return String(value);
  }

  return date.toLocaleTimeString();
}

// --------------------------------------------------
// Get data from MongoDB
// --------------------------------------------------

async function getDashboardData(db) {
  // Latest Flink 1-minute window
  const latestMetric = await db
    .collection("order_metrics_1min")
    .find({})
    .sort({ window_end: -1 })
    .limit(1)
    .next();

  // High-value orders
  const highValueOrders = await db
    .collection("high_value_orders")
    .find({})
    .sort({ timestamp: -1 })
    .limit(5)
    .toArray();

  // Customer summaries
  const customerSummaries = await db
    .collection("customer_order_summary")
    .find({})
    .sort({ total_amount: -1 })
    .limit(5)
    .toArray();

  // All Flink windows
  const metrics = await db.collection("order_metrics_1min").find({}).toArray();

  // Calculate total orders and revenue
  let totalOrders = 0;
  let totalRevenue = 0;

  for (const metric of metrics) {
    totalOrders += toNumber(metric.order_count);

    totalRevenue += toNumber(metric.total_sales);
  }

  const averageOrderValue = totalOrders > 0 ? totalRevenue / totalOrders : 0;

  // Latest available order
  const lastOrder = highValueOrders.length > 0 ? highValueOrders[0] : null;

  return {
    latestMetric,
    highValueOrders,
    customerSummaries,
    totalOrders,
    totalRevenue,
    averageOrderValue,
    lastOrder,
  };
}

// --------------------------------------------------
// Display dashboard
// --------------------------------------------------

function renderDashboard(data) {
  console.clear();

  console.log(
    "================================================================",
  );

  console.log("       REAL-TIME E-COMMERCE STREAMING DASHBOARD");

  console.log(
    "================================================================",
  );

  console.log(`Updated: ${new Date().toLocaleTimeString()}`);

  // --------------------------------------------------
  // Kafka / Orders
  // --------------------------------------------------

  console.log("\nKAFKA / ORDERS");

  console.log(
    "----------------------------------------------------------------",
  );

  console.log(`Total orders received : ${data.totalOrders}`);

  console.log(`Total revenue         : ${money(data.totalRevenue)}`);

  console.log(`Average order value   : ${money(data.averageOrderValue)}`);

  console.log(`Last order            : ${data.lastOrder?.orderId || "N/A"}`);

  // --------------------------------------------------
  // Flink
  // --------------------------------------------------

  console.log("\nFLINK - 1 MINUTE WINDOW");

  console.log(
    "----------------------------------------------------------------",
  );

  if (data.latestMetric) {
    console.log(`Window start : ${formatTime(data.latestMetric.window_start)}`);

    console.log(`Window end   : ${formatTime(data.latestMetric.window_end)}`);

    console.log(`Orders       : ${toNumber(data.latestMetric.order_count)}`);

    console.log(`Total sales  : ${money(data.latestMetric.total_sales)}`);
  } else {
    console.log("No Flink window data yet.");
  }

  // --------------------------------------------------
  // High-value orders
  // --------------------------------------------------

  console.log("\nHIGH VALUE ORDERS");

  console.log(
    "----------------------------------------------------------------",
  );

  if (data.highValueOrders.length === 0) {
    console.log("No high-value orders yet.");
  } else {
    for (const order of data.highValueOrders) {
      console.log(
        `${(order.orderId || "N/A").padEnd(20)} ` +
          `${(order.product || "N/A").padEnd(12)} ` +
          `${(order.customerId || "N/A").padEnd(12)} ` +
          `${money(order.amount)}`,
      );
    }
  }

  // --------------------------------------------------
  // Customer summaries
  // --------------------------------------------------

  console.log("\nCUSTOMER ORDER SUMMARY");

  console.log(
    "----------------------------------------------------------------",
  );

  if (data.customerSummaries.length === 0) {
    console.log("No customer summary data yet.");
  } else {
    for (const customer of data.customerSummaries) {
      console.log(
        `${(customer.customerId || "Unknown").padEnd(12)} ` +
          `Orders: ${String(toNumber(customer.order_count)).padEnd(6)} ` +
          `Total: ${money(customer.total_amount)}`,
      );
    }
  }

  console.log(
    "\n================================================================",
  );

  console.log("Streaming... Press Ctrl+C to stop.");
}

// --------------------------------------------------
// Main
// --------------------------------------------------

async function main() {
  try {
    console.log("Connecting to MongoDB Atlas...");

    await client.connect();

    console.log("Connected!");

    const db = client.db(dbName);

    // First dashboard update
    const data = await getDashboardData(db);

    renderDashboard(data);

    // Refresh every 2 seconds
    setInterval(async () => {
      try {
        const data = await getDashboardData(db);

        renderDashboard(data);
      } catch (error) {
        console.error("Dashboard refresh error:", error.message);
      }
    }, 2000);
  } catch (error) {
    console.error("MongoDB connection failed:");

    console.error(error.message);

    process.exit(1);
  }
}

// --------------------------------------------------
// Graceful shutdown
// --------------------------------------------------

process.on("SIGINT", async () => {
  console.log("\nStopping dashboard...");

  await client.close();

  process.exit(0);
});

main();
