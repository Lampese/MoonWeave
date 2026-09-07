///|
struct Order {
  store : String
  unit_price_cents : Int64
  quantity : Int64
  cancelled : Bool
}

///|
struct WindowRevenue {
  store : String
  revenue_cents : Int64
}

///|
struct RevenueOrder {
  store : String
  revenue_cents : Int64
  cancelled : Bool
}

///|
fn main raise {
  // 这是一个按门店统计订单营业额的正常计算任务。
  let source = @flow.Source::from_array([
    Order::{
      store: "north",
      unit_price_cents: 1200L,
      quantity: 2L,
      cancelled: false,
    },
    Order::{
      store: "north",
      unit_price_cents: 800L,
      quantity: 1L,
      cancelled: false,
    },
    Order::{
      store: "north",
      unit_price_cents: 500L,
      quantity: 1L,
      cancelled: false,
    },
    Order::{
      store: "south",
      unit_price_cents: 1500L,
      quantity: 1L,
      cancelled: false,
    },
    Order::{
      store: "south",
      unit_price_cents: 700L,
      quantity: 2L,
      cancelled: true,
    },
    Order::{
      store: "south",
      unit_price_cents: 900L,
      quantity: 1L,
      cancelled: false,
    },
  ])

  // map 计算订单金额，filter 排除已取消订单。
  let revenue_stream = source
    .stream()
    .map(order => {
      store: order.store,
      revenue_cents: order.unit_price_cents * order.quantity,
      cancelled: order.cancelled,
    })
    .filter(order => !order.cancelled)

  // 同一个门店进入同一个 key，count window 每两个订单输出一次汇总。
  let aggregate : @flow.Aggregate[RevenueOrder, Int64, WindowRevenue] = @flow.Aggregate::new(
    _ => 0L,
    (total, order) => total + order.revenue_cents,
    (store, total) => WindowRevenue::{ store, revenue_cents: total, },
  )
  let handle = revenue_stream
    .key_by_fn(order => order.store)
    .window(@flow.Window::count(2))
    .aggregate(aggregate)
    .collect()

  println("status=\{@debug.to_string(handle.status())}")
  for result in handle.outputs() {
    println(
      "window store=\{result.store} revenue_cents=\{result.revenue_cents}",
    )
  }
  handle.close()
  println("status_after_close=\{@debug.to_string(handle.status())}")
}
