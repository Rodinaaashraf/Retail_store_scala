import scala.io.Source
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.io.{File, FileWriter, PrintWriter}
import java.sql.{Connection, DriverManager, PreparedStatement}

// Logger setup
//creating a file named "rules_engine.log" for our logs
val logFile = new File("rules_engine.log")
val writer = new PrintWriter(logFile)

//function for the logs that appends on the file the messages  with timestamp and log level and if the file if not exits create it
def log(level: String, message: String): Unit = {
  val timestamp = LocalDateTime.now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  val logMessage = s"$timestamp $level $message"
  val writer = new PrintWriter(new FileWriter("F:\\scala_project\\Retail_store_scala\\rules_engine.log", true))
  writer.println(logMessage)
  writer.close()
}
//Represents a single order with fields corresponding to columns in the CSV file
case class Order(timestamp: String,
                 productName: String,
                 expiryDate: String,
                 quantity: Int,
                 unitPrice: Float,
                 channel: String,
                 paymentMethod: String)

log("INFO", "Starting rules engine processing")

//reading the csv file
val lines = Source.fromFile("F:\\scala_project\\Retail_store_scala\\TRX1000.csv").getLines().toList.tail
log("INFO", s"Successfully read ${lines.size} orders from input file")

// Converts a line from the CSV into an Order object by splitting fields
def parseLineToOrder(line: String): Order = {
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
}

// Apply the parsing function to each line to obtain a list of Order objects
val orders = lines.map(parseLineToOrder)
log("INFO", s"Successfully parsed ${orders.size} orders")

//this function to extract the day and month from timestamp (we need this in the lessThan30 function)
def splitDate(o: Order): (Int, Int) = {
  val date = o.timestamp.split("T")(0).split("-")
  (date(1).toInt, date(2).toInt)
}
//this function to extract the day and month from expiry date
def splitExpireDate(o: Order): (Int, Int) = {
  val expirydate = o.expiryDate.split("-")
  (expirydate(1).toInt, expirydate(2).toInt)
}
//the whole data in 2023 so  if the expirydate and the timestamp on the same month this means that the days left less than 30
def lessThan30(o: Order): Boolean = {
  val (month, day) = splitDate(o)
  //using '_' cuz i care only about the first value "expMonth"
  val (expMonth, _) = splitExpireDate(o)
  expMonth == month
}
//this is the calculation for the first condition
def lessThan30Discount(o: Order): Double = {
  val (month, day) = splitDate(o)
  val (_, expDay) = splitExpireDate(o)
  //formula ashan mn3mlsh 30 if
  (30 - (expDay - day)) / 100
}
//this function to check if the 2 words exists in the product name and i used contains not split cuz the place of the wine and cheese wokrds is not always fixed
def cheeseAndwineProducts(o: Order): Boolean = {
  o.productName.contains("Wine") || o.productName.contains("Cheese")
}

def cheeseAndwineProductsDiscount(o: Order): Double = {
  if (o.productName.contains("Wine")) 0.05 else 0.1
}
//use "val (month, day) = splitDate(o)" by using the splitDate function isntead of replicating the code this why we did the split function from the beggining
def productsSoldon23March(o: Order): Boolean = {
  val (month, day) = splitDate(o)
  month == 3 && day == 23
}

def productsSoldon23MarchDiscount(o: Order): Double = 0.5

def moreThan5Products(o: Order): Boolean = o.quantity > 5

def moreThan5ProductsDiscount(o: Order): Double = o.quantity match {
  case q if q >= 6 && q <= 9  => 0.05
  case q if q >= 10 && q <= 14 => 0.07
  case q if q >= 15 => 0.10
  case _ => 0.0
}

def channelIsApp(o: Order): Boolean = o.channel == "App"

def channelIsAppDiscount(o: Order): Double = {
  //this a formula used to calculate discount based on quantity:
  // Starts at 5%, then adds 5% for every additional 5 items (after the first)
  val discount = ((o.quantity - 1) / 5) + 1
  discount * 0.05
}

def paymentMethodIsvisa(o: Order): Boolean = o.paymentMethod == "Visa"

def paymentMethodIsvisaDiscount(o: Order): Double = 0.05


// a condition: a function that determines if the rule applies to an order
// a discount: a function that calculates the discount percentage
// a name for logging or debugging purposes
case class DiscountRule(condition: Order => Boolean, discount: Order => Double, name: String)

// List of Discount Rules
// Each rule uses a higher-order function for both condition checking and discount calculation
// These functions are defined elsewhere (e.g moreThan5Products)
val discountRules: List[DiscountRule] = List(
  DiscountRule(moreThan5Products, moreThan5ProductsDiscount, "Quantity > 5 Discount"),
  DiscountRule(channelIsApp, channelIsAppDiscount, "App Channel Discount"),
  DiscountRule(paymentMethodIsvisa, paymentMethodIsvisaDiscount, "Visa Payment Discount"),
  DiscountRule(cheeseAndwineProducts, cheeseAndwineProductsDiscount, "Cheese/Wine Product Discount"),
  DiscountRule(productsSoldon23March, productsSoldon23MarchDiscount, "March 23 Discount"),
  DiscountRule(lessThan30, lessThan30Discount, "Expiry <30 Days Discount")
)

log("INFO", s"Loaded ${discountRules.size} discount rules")


//function de bastkhdmha ashan kol order andy hydkhol aleha yshouf lw howa by qualify lel condition el mwgod fl list "discountRules" ybaa okay e3mly el calculation lel discount
def calculateTotalDiscount(o: Order): Double = {
  val applicableDiscounts = discountRules
    .filter(rule => rule.condition(o))    // filter: selects only the rules whose condition returns true for the order.
    .map(rule => rule.discount(o))        //  map: applies the corresponding discount function to each applicable rule.
    .sorted(Ordering[Double].reverse)   // sorted to select the top two highest discount if we have a product that qualifies for more than 1 condition
  val toptwo = applicableDiscounts.take(2)
  if (toptwo.isEmpty) 0.0 else toptwo.sum / toptwo.length //return the average of the top two discounts using "toptwo.length" instead if 2 to make it dynamic
}


log("INFO", "Starting order processing")

orders.foreach { order =>
  log("INFO", s"Processing order: ${order.productName}, Quantity: ${order.quantity}")
  val discount = calculateTotalDiscount(order)
  val totalPrice = order.unitPrice * order.quantity // Compute total price of the product before discount
  val discountAmount = totalPrice * discount //compure the discount amount
  val finalPrice = totalPrice - discountAmount //final price after the discount

  log("INFO", f"Order processed - Product: ${order.productName}, Quantity: ${order.quantity}, " +
    f"Discount Amount: $$${discountAmount}%.2f, Final Price: $$${finalPrice}%.2f")
  println(f"Product: ${order.productName}, Quantity: ${order.quantity}, " +
    f"Discount Amount: $$${discountAmount}%.2f, Final Price: $$${finalPrice}%.2f")
  log("INFO", f"Processed order for '${order.productName}', final price: $$${finalPrice}%.2f after $$${discountAmount}%.2f discount")
}

// DB Connection
val url = "jdbc:oracle:thin:@localhost:1521:xe"
val username = "HR"
val password = "hr"

//oracle driver also dont forget to put the oracle jar inside the IJ dependencies
Class.forName("oracle.jdbc.driver.OracleDriver")
val connection: Connection = DriverManager.getConnection(url, username, password)
connection.setAutoCommit(false)

val insertSQL =
  """
    |INSERT INTO processed_orders (
    |  order_timestamp, product_name, expiry_date, quantity,
    |  unit_price, channel, payment_method,
    |  discount_percent, discount_amount, final_price
    |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """.stripMargin

val preparedStatement: PreparedStatement = connection.prepareStatement(insertSQL)

orders.foreach { order =>
  val discount = calculateTotalDiscount(order)
  val totalPrice = order.unitPrice * order.quantity
  val discountAmount = totalPrice * discount
  val finalPrice = totalPrice - discountAmount
  val expiry = java.sql.Date.valueOf(order.expiryDate)

  preparedStatement.setString(1, order.timestamp)
  preparedStatement.setString(2, order.productName)
  preparedStatement.setDate(3, expiry)
  preparedStatement.setInt(4, order.quantity)
  preparedStatement.setFloat(5, order.unitPrice)
  preparedStatement.setString(6, order.channel)
  preparedStatement.setString(7, order.paymentMethod)
  preparedStatement.setDouble(8, discount)
  preparedStatement.setDouble(9, discountAmount)
  preparedStatement.setDouble(10, finalPrice)

  preparedStatement.executeUpdate()
}

connection.commit()
preparedStatement.close()
connection.close()

log("INFO", "Rules engine processing completed and results inserted into Oracle")
writer.close()
