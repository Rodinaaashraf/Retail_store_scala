#Retail Store Discount Rules Engine

### 📌 Problem Statement

A major retail store aims to implement a Rules Engine to determine discount eligibility for orders based on specific business logic. Each order must be evaluated against several qualifying rules, and the **appropriate discount** should be computed using **pure functional programming principles**.

---

###Project Objectives

- Evaluate retail orders and assign appropriate discounts based on qualifying rules.
- If an order qualifies for multiple discounts, apply the **top two** and compute their average.
- Compute the **final price** after applying the discount.
- **Persist** the final order details into a database.
- **Log** each step of the Rules Engine’s operation with a consistent timestamped format.

---

###Functional Design Overview

- `Order` case class models a transaction.
- `DiscountRule` encapsulates:
  - A **condition function**: `Order => Boolean`
  - A **discount function**: `Order => Double`
- Rules Engine:
  - Reads data from a **CSV file**
  - Parses and evaluates each order
  - Applies the **top 2 applicable discounts**
  - Logs process and writes result to **DB**

---

### 🔧 Technical Highlights Summary

####Pure Functional Programming (FP) Principles

- All core logic is written using **pure functions**: functions depend only on input and have **no side effects**.
- No `var`, no mutable data structures, and no loops are used.
- Functional constructs like `map`, `filter`, and `case classes` are leveraged for **immutability** and **clarity**.

#### Logging System

- A custom logging function logs all engine events to `rules_engine.log`.
- Log format strictly follows:[YYYY-MM-DD HH:MM:SS]



###Database Integration (Oracle JDBC)

```sql

CREATE TABLE processed_orders (

    order_id VARCHAR2(50) PRIMARY KEY,

    customer_name VARCHAR2(100),

    original_price NUMBER(10,2),

    discount1 NUMBER(5,2),

    discount2 NUMBER(5,2),

    final_price NUMBER(10,2)
