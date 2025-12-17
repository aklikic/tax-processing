# Tax Year Batch Processing: Book Cost & Gain/Loss Calculation

## Purpose

The purpose of this is to batch process a full tax year worth of data and generate two distinct outcomes:

1. Compute the Book cost of a client's position accurately
2. Record the gain/loss values that are based off an accurate book cost

The first stab will be a batch process but in future could be extended to be the live representation of this data and have transactions feed in via events from our system. The objective would be to determine how we can improve batch processing performance and ensure that this can scale. I've attached some calcs to help us understand the scale requirement and talk through how Akka could help improve this.

## Processing Steps

### Start with Opening Balances

**Number of entities:** 4,427,234

### Step 1: Opening Balances Take-on

Per holding, opening units and opening cost

**NB:** Opening cost and units will be the basis for the rest of the calculations

- **Book cost** = Opening cost
- **Cents per unit** = Opening Cost / Opening Units

**Produce event:** `book_cost_adjusted`
```
{
    account id
    instrument id
    transaction {
        id
        type
        date/time
        units
        price
        fees
    }
    units held
    book cost
    cents per unit
}
```

### Step 2: Process Stream of Transactions

**Number of transactions:** 12,314,040

## Treatment of Transactions

### CASE 1: Buy

- **Increase units:** Opening Units + units bought
- **Update book cost:** Opening cost + ((units bought × price) + fees)
- **Update cents per unit:** Updated book cost / Increased units
- **Produce event:** `book_cost_adjusted`

### CASE 2: Sell

- **Net Proceeds** = (Units sold × price) - fees
- **Gain/Loss** = Net Proceeds - (cents per unit × Units sold)

**Produce event:** `gain_loss_incurred`
```
{
    account id
    instrument id
    transaction {
        id
        type
        date/time
        units
        price
        fees
    }
    gain or loss amount
}
```

- **Reduce book cost:** Book cost - (units sold × Cost per unit)
- **Decrease units:** Units - Units sold
- **Produce event:** `book_cost_adjusted`

### CASE 3: Transfer In

- **Increase units:** Units + units transferred in
- **Increased book cost:** Book cost + (units transferred in × price)
- **Update cents per unit:** Increased book cost / Increased units
- **Produce event:** `book_cost_adjusted`

### CASE 4: Transfer Out

- **Decrease units:** Units - units transferred out
- **Decreased book cost:** Book cost - (units transferred out × price)
- **Update cents per unit:** Decreased book cost / Decreased units
- **Produce event:** `book_cost_adjusted`

### CASE 5: Corporate Action Event (Cash Only)

- **Adjusted book cost:** Book cost - (units × proceeds)
- **Update cents per unit:** Adjusted book cost / Units
- **Produce event:** `book_cost_adjusted`