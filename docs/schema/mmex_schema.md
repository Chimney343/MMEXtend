# MMEX (.mmb) Database Schema Reference
Source: https://github.com/moneymanagerex/database (tables.sql, v21)
Format: SQLite3. File extension is `.mmb`.

> **Usage note for AI agents:** Read this file FIRST when debugging any issue
> that touches the MMEX database structure, or when the user asks about table
> relations, column names, or field meanings. Only suggest this file may be
> outdated if the issue persists after applying the schema information here.

---

## Core Transaction Tables

### CHECKINGACCOUNT_V1 — all transactions
| Column | Type | Notes |
|---|---|---|
| TRANSID | integer PK | |
| ACCOUNTID | integer NOT NULL | → ACCOUNTLIST_V1 |
| TOACCOUNTID | integer | → ACCOUNTLIST_V1 (transfers) |
| PAYEEID | integer NOT NULL | → PAYEE_V1 |
| TRANSCODE | TEXT NOT NULL | Withdrawal, Deposit, Transfer |
| TRANSAMOUNT | numeric NOT NULL | Amount in account currency |
| TOTRANSAMOUNT | numeric | Amount in dest account currency (Transfer) |
| STATUS | TEXT | None, Reconciled, Void, Follow up, Duplicate |
| TRANSACTIONNUMBER | TEXT | |
| NOTES | TEXT | |
| CATEGID | integer | → CATEGORY_V1 (null if split) |
| TRANSDATE | TEXT | ISO date |
| LASTUPDATEDTIME | TEXT | |
| DELETEDTIME | TEXT | |
| FOLLOWUPID | integer | |
| COLOR | integer | DEFAULT -1 |

### SPLITTRANSACTIONS_V1 — split transaction lines
| Column | Type | Notes |
|---|---|---|
| SPLITTRANSID | integer PK | |
| TRANSID | integer NOT NULL | → CHECKINGACCOUNT_V1 |
| CATEGID | integer | → CATEGORY_V1 |
| SPLITTRANSAMOUNT | numeric | |
| NOTES | TEXT | |

---

## Account & Currency Tables

### ACCOUNTLIST_V1 — bank/investment accounts
| Column | Type | Notes |
|---|---|---|
| ACCOUNTID | integer PK | |
| ACCOUNTNAME | TEXT UNIQUE | |
| ACCOUNTTYPE | TEXT NOT NULL | Cash, Checking, Term, Investment, Credit Card, Loan, Asset, Shares |
| ACCOUNTNUM | TEXT | |
| STATUS | TEXT NOT NULL | Open, Closed |
| CURRENCYID | integer NOT NULL | → CURRENCYFORMATS_V1 |
| INITIALBAL | numeric | |
| INITIALDATE | TEXT | |
| FAVORITEACCT | TEXT NOT NULL | |
| CREDITLIMIT | numeric | |
| INTERESTRATE | numeric | |
| MINIMUMBALANCE | numeric | |
| STATEMENTLOCKED | integer | |
| STATEMENTDATE | TEXT | |

### CURRENCYFORMATS_V1 — currencies
| Column | Type | Notes |
|---|---|---|
| CURRENCYID | integer PK | |
| CURRENCYNAME | TEXT UNIQUE | |
| CURRENCY_SYMBOL | TEXT UNIQUE | e.g. USD, EUR |
| CURRENCY_TYPE | TEXT NOT NULL | Fiat, Crypto |
| PFX_SYMBOL / SFX_SYMBOL | TEXT | Prefix/suffix display symbol |
| SCALE | integer | Cents per unit (100 = 2 decimals) |
| BASECONVRATE | numeric | Relative to base currency |

### CURRENCYHISTORY_V1 — historical exchange rates
| Column | Type |
|---|---|
| CURRHISTID | INTEGER PK |
| CURRENCYID | INTEGER NOT NULL |
| CURRDATE | TEXT NOT NULL |
| CURRVALUE | NUMERIC NOT NULL |
| CURRUPDTYPE | INTEGER |
| UNIQUE | (CURRENCYID, CURRDATE) |

---

## Classification Tables

### CATEGORY_V1 — hierarchical categories
| Column | Type | Notes |
|---|---|---|
| CATEGID | INTEGER PK | |
| CATEGNAME | TEXT NOT NULL | |
| ACTIVE | INTEGER | |
| PARENTID | INTEGER | -1 = top-level |
| UNIQUE | (CATEGNAME, PARENTID) | |

### PAYEE_V1 — payees
| Column | Type | Notes |
|---|---|---|
| PAYEEID | integer PK | |
| PAYEENAME | TEXT UNIQUE | |
| CATEGID | integer | Default category |
| NUMBER / WEBSITE / NOTES | TEXT | |
| ACTIVE | integer | |
| PATTERN | TEXT | Auto-categorisation pattern |

### TAG_V1 — tags
| Column | Notes |
|---|---|
| TAGID INTEGER PK | |
| TAGNAME TEXT UNIQUE | |
| ACTIVE INTEGER | |

### TAGLINK_V1 — tag associations (polymorphic)
| Column | Notes |
|---|---|
| TAGLINKID INTEGER PK | |
| REFTYPE TEXT NOT NULL | Transaction, Stock, Asset, … |
| REFID INTEGER NOT NULL | FK to target table (polymorphic) |
| TAGID INTEGER NOT NULL | → TAG_V1 |

---

## Recurring Transactions (Bills & Deposits)

### BILLSDEPOSITS_V1
Same core columns as CHECKINGACCOUNT_V1 plus:
| Column | Notes |
|---|---|
| REPEATS | integer — recurrence type |
| NEXTOCCURRENCEDATE | TEXT |
| NUMOCCURRENCES | integer |

### BUDGETSPLITTRANSACTIONS_V1 — split lines for recurring
Same shape as SPLITTRANSACTIONS_V1, with TRANSID → BILLSDEPOSITS_V1.

---

## Budget Tables

### BUDGETYEAR_V1
| BUDGETYEARID PK | BUDGETYEARNAME TEXT UNIQUE |

### BUDGETTABLE_V1
| Column | Notes |
|---|---|
| BUDGETENTRYID PK | |
| BUDGETYEARID | → BUDGETYEAR_V1 |
| CATEGID | → CATEGORY_V1 |
| PERIOD | None, Weekly, Bi-Weekly, Monthly, Bi-Monthly, Quarterly, Half-Yearly, Yearly, Daily |
| AMOUNT numeric | |
| ACTIVE integer | |

---

## Investment Tables

### STOCK_V1 — holdings
| Column | Notes |
|---|---|
| STOCKID PK | |
| HELDAT | → ACCOUNTLIST_V1 |
| PURCHASEDATE TEXT | |
| STOCKNAME TEXT | |
| SYMBOL TEXT | |
| NUMSHARES numeric | |
| PURCHASEPRICE numeric | |
| CURRENTPRICE numeric | |
| VALUE numeric | |
| COMMISSION numeric | |

### STOCKHISTORY_V1 — price history
| HISTID PK | SYMBOL TEXT | DATE TEXT | VALUE numeric | UPDTYPE integer |
UNIQUE(SYMBOL, DATE)

### SHAREINFO_V1 — share transaction details
| Column | Notes |
|---|---|
| SHAREINFOID PK | |
| CHECKINGACCOUNTID | → CHECKINGACCOUNT_V1 |
| SHARENUMBER numeric | |
| SHAREPRICE numeric | |
| SHARECOMMISSION numeric | |
| SHARELOT TEXT | |

### TRANSLINK_V1 — links transactions to assets/stocks
| Column | Notes |
|---|---|
| TRANSLINKID PK | |
| CHECKINGACCOUNTID | → CHECKINGACCOUNT_V1 |
| LINKTYPE TEXT | Asset, Stock |
| LINKRECORDID integer | → ASSETS_V1 or STOCK_V1 (polymorphic) |

---

## Assets

### ASSETS_V1
| Column | Notes |
|---|---|
| ASSETID PK | |
| STARTDATE TEXT | |
| ASSETNAME TEXT | |
| ASSETSTATUS TEXT | Open, Closed |
| CURRENCYID integer | → CURRENCYFORMATS_V1 |
| VALUE numeric | |
| VALUECHANGE TEXT | None, Appreciates, Depreciates |
| VALUECHANGEMODE TEXT | Percentage, Linear |
| VALUECHANGERATE numeric | |
| ASSETTYPE TEXT | Property, Automobile, Household Object, Art, Jewellery, Cash, Other |

---

## Custom Fields

### CUSTOMFIELD_V1 — field definitions
| Column | Notes |
|---|---|
| FIELDID PK | |
| REFTYPE TEXT | Transaction, Stock, Asset, Bank Account, Repeating Transaction, Payee |
| DESCRIPTION TEXT | |
| TYPE TEXT | String, Integer, Decimal, Boolean, Date, Time, SingleChoice, MultiChoice |
| PROPERTIES TEXT | JSON |

### CUSTOMFIELDDATA_V1 — field values
| Column | Notes |
|---|---|
| FIELDATADID PK | |
| FIELDID | → CUSTOMFIELD_V1 |
| REFID integer | → target row (polymorphic on CUSTOMFIELD_V1.REFTYPE) |
| CONTENT TEXT | |

---

## Metadata / Settings

### INFOTABLE_V1 — database metadata
| INFOID PK | INFONAME TEXT UNIQUE | INFOVALUE TEXT |
Key entry: `DATAVERSION = '3'`

### SETTING_V1 — app settings
| SETTINGID PK | SETTINGNAME TEXT UNIQUE | SETTINGVALUE TEXT |

### REPORT_V1 — saved SQL reports
| Column | Notes |
|---|---|
| REPORTID PK | |
| REPORTNAME TEXT UNIQUE | |
| GROUPNAME TEXT | |
| SQLCONTENT TEXT | |
| LUACONTENT TEXT | |
| TEMPLATECONTENT TEXT | |

### ATTACHMENT_V1 — file attachments (polymorphic)
| Column | Notes |
|---|---|
| ATTACHMENTID PK | |
| REFTYPE TEXT | Transaction, Stock, Asset, Bank Account, Repeating Transaction, Payee |
| REFID INTEGER | → target table |
| DESCRIPTION TEXT | |
| FILENAME TEXT | |

### USAGE_V1 — telemetry / usage log
| USAGEID PK | USAGEDATE TEXT | JSONCONTENT TEXT |

---

## Key Relationships Summary
```
CHECKINGACCOUNT_V1.ACCOUNTID      → ACCOUNTLIST_V1.ACCOUNTID
CHECKINGACCOUNT_V1.TOACCOUNTID    → ACCOUNTLIST_V1.ACCOUNTID
CHECKINGACCOUNT_V1.PAYEEID        → PAYEE_V1.PAYEEID
CHECKINGACCOUNT_V1.CATEGID        → CATEGORY_V1.CATEGID
SPLITTRANSACTIONS_V1.TRANSID      → CHECKINGACCOUNT_V1.TRANSID
SPLITTRANSACTIONS_V1.CATEGID      → CATEGORY_V1.CATEGID
ACCOUNTLIST_V1.CURRENCYID         → CURRENCYFORMATS_V1.CURRENCYID
CATEGORY_V1.PARENTID              → CATEGORY_V1.CATEGID  (-1 = root)
STOCK_V1.HELDAT                   → ACCOUNTLIST_V1.ACCOUNTID
BUDGETTABLE_V1.BUDGETYEARID       → BUDGETYEAR_V1.BUDGETYEARID
BUDGETTABLE_V1.CATEGID            → CATEGORY_V1.CATEGID
TRANSLINK_V1.CHECKINGACCOUNTID    → CHECKINGACCOUNT_V1.TRANSID
SHAREINFO_V1.CHECKINGACCOUNTID    → CHECKINGACCOUNT_V1.TRANSID
TAGLINK_V1.TAGID                  → TAG_V1.TAGID
CUSTOMFIELDDATA_V1.FIELDID        → CUSTOMFIELD_V1.FIELDID
```

## Polymorphic REFTYPE Values
REFTYPE appears in: ATTACHMENT_V1, CUSTOMFIELD_V1/DATA, TAGLINK_V1, TRANSLINK_V1
Values: `Transaction`, `Stock`, `Asset`, `Bank Account`, `Repeating Transaction`, `Payee`
