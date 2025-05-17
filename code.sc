import scala.io.Source
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.io.{File, FileWriter, PrintWriter}

// Logger setup
val logFile = new File("rules_engine.log")
val writer = new PrintWriter(logFile)

// Logger function
def log(level: String, message: String): Unit = {
  val timestamp = LocalDateTime.now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  val logMessage = s"$timestamp $level $message"
  val writer = new PrintWriter(new FileWriter("F:\\scala_project\\rules_engine.log", true))
  writer.println(logMessage)
  writer.close()
}
try {
  case class Order(timestamp: String,
                   productName: String,
                   expiryDate: String,
                   quantity: Int,
                   unitPrice: Float,
                   channel: String,
                   paymentMethod: String)

  log("INFO", "Starting rules engine processing")

  val lines = try {
    Source.fromFile("F:\\scala_project\\TRX1000.csv").getLines().toList.tail
  } catch {
    case e: Exception =>
      log("ERROR", s"Failed to read input file: ${e.getMessage}")
      throw e
  }

  log("INFO", s"Successfully read ${lines.size} orders from input file")

  def parseLineToOrder(line: String): Order = {
    try {
      val fields = line.split(",")
      Order(
        timestamp = fields(0),
        productName = fields(1),
        expiryDate = fields(2),
        quantity = fields(3).toInt,
        unitPrice = fields(4).toFloat,
        channel = fields(5),
        paymentMethod = fields(6)
      )
    } catch {
      case e: Exception =>
        log("ERROR", s"Failed to parse line: $line - ${e.getMessage}")
        throw e
    }
  }

  val orders = lines.map(parseLineToOrder)
  log("INFO", s"Successfully parsed ${orders.size} orders")


  def splitDate(o: Order): (Int, Int) = {
    val date = o.timestamp.split("T")(0).split("-")
    val month = date(1).toInt
    val day = date(2).toInt
    (month, day)
  }

  def splitExpireDate(o: Order): (Int, Int) = {
    val expirydate = o.expiryDate.split("-")
    val month = expirydate(1).toInt
    val day = expirydate(2).toInt
    (month, day)
  }

  def lessThan30(o: Order): Boolean = {
    val (month, day) = splitDate(o)
    val (expMonth, expDay) = splitExpireDate(o)
    expMonth == month
  }

  def lessThan30Discount(o: Order): Double = {
    val (month, day) = splitDate(o)
    val (expMonth, expDay) = splitExpireDate(o)
    (30 - (expDay - day)) / 100.0
  }

  def cheeseAndwineProducts(o: Order): Boolean = {
    o.productName.contains("Wine") || o.productName.contains("Cheese")
  }

  def cheeseAndwineProductsDiscount(o: Order): Double = {
    if (o.productName.contains("Wine")) 0.05 else 0.1
  }

  def productsSoldon23March(o: Order): Boolean = {
    val (month, day) = splitDate(o)
    month == 3 && day == 23
  }

  def productsSoldon23MarchDiscount(o: Order): Double = {
    0.5
  }

  def moreThan5Products(o: Order): Boolean = {
    o.quantity > 5
  }

  def moreThan5ProductsDiscount(o: Order): Double = {
    o.quantity match {
      case q if q >= 6 && q <= 9  => 0.05
      case q if q >= 10 && q <= 14 => 0.07
      case q if q >= 15 => 0.10
      case _ => 0.0
    }
  }

  def channelIsApp(o: Order): Boolean = {
    o.channel == "App"
  }

  def channelIsAppDiscount(o: Order): Double = {
    val discount =((o.quantity - 1) / 5) + 1
     return discount * 0.05
  }

  def paymentMethodIsvisa(o: Order): Boolean = {
    o.paymentMethod == "Visa"
  }

  def paymentMethodIsvisaDiscount(o: Order): Double = {
    0.05
  }

  case class DiscountRule(condition: Order => Boolean, discount: Order => Double, name: String)

  val discountRules: List[DiscountRule] = List(
    DiscountRule(moreThan5Products, moreThan5ProductsDiscount, "Quantity > 5 Discount"),
    DiscountRule(channelIsApp, channelIsAppDiscount, "App Channel Discount"),
    DiscountRule(paymentMethodIsvisa, paymentMethodIsvisaDiscount, "Visa Payment Discount"),
    DiscountRule(cheeseAndwineProducts, cheeseAndwineProductsDiscount, "Cheese/Wine Product Discount"),
    DiscountRule(productsSoldon23March, productsSoldon23MarchDiscount, "March 23 Discount"),
    DiscountRule(lessThan30, lessThan30Discount, "Expiry <30 Days Discount")
  )

  log("INFO", s"Loaded ${discountRules.size} discount rules")

  def calculateTotalDiscount(o: Order): Double = {
    val applicableDiscounts = discountRules
      .filter(rule => {
        val applies = rule.condition(o)
        if (applies) log("DEBUG", s"Rule '${rule.name}' applies to order: ${o.productName}")
        applies
      })
      .map(rule => {
        val discountValue = rule.discount(o)
        log("DEBUG", s"Calculated ${rule.name} discount value: $discountValue for product: ${o.productName}")
        discountValue
      })
      .sorted(Ordering[Double].reverse)

    val toptwo = applicableDiscounts.take(2)

    if (toptwo.isEmpty) {
      0.0
    } else {
      val avgDiscount = toptwo.sum / toptwo.length
      avgDiscount
    }
  }

  log("INFO", "Starting order processing")

  orders.foreach { order =>
    try {
      log("INFO", s"Processing order: ${order.productName}, Quantity: ${order.quantity}")

      val discount = calculateTotalDiscount(order)
      val totalPrice = order.unitPrice * order.quantity
      val discountAmount = totalPrice * discount
      val finalPrice = totalPrice - discountAmount

      log("INFO", f"Order processed - Product: ${order.productName}, Quantity: ${order.quantity}, " +
        f"Discount Amount: $$${discountAmount}%.2f, Final Price: $$${finalPrice}%.2f")

      println(f"Product: ${order.productName}, Quantity: ${order.quantity}, " +
        f"Discount Amount: $$${discountAmount}%.2f, Final Price: $$${finalPrice}%.2f")
      log("INFO", f"Processed order for '${order.productName}', final price: $$${finalPrice}%.2f after $$${discountAmount}%.2f discount")
    } catch {
      case e: Exception =>
        log("ERROR", s"Failed to process order ${order.productName}: ${e.getMessage}")
    }
  }

  log("INFO", s"Successfully processed ${orders.size} orders")
} finally {
  writer.close()
  log("INFO", "Rules engine processing completed")
}