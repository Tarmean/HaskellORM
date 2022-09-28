# [ORM][]

Experiment with proper object relational mappings in Haskell. Three points

- Easy joins through foreign keys
- Automatically parse into nested results
- Allow easy updates of nested types by diffing

Example query:

```Haskell
testQ :: QueryM (Result (Customer, [Account]))
testQ = queryRoot $ do
    cust <- SQL.query customer
    vaccs <- nested cust.accounts $ \acc -> do
       SQL.wheres (acc.availBalance .>. value (Just (25000::Double))) 
       pure (sel acc)
    pure $ do
        c <- sel cust
        accs <- vaccs
        pure (c, accs)
```
This builds on relational-query for the query building monad. We add nested queries.
Each `nested` adds an extra query, so we run roughly the following queries:

```SQL
SELECT C.* FROM Customers C
SELECT A.customer_id, A.* FROM Account A WHERE A.availBalance > 25000 AND A.customer_id IN (?,?,?,...) 
```

The `cust.accounts` uses -XRecordDotSyntax to resolve the join metadata, but there is no real field.

The process goes:

- Run the query builder monad. Each `nested` receives an identifier and is stashed into a Typeable map.
- After the root query is run and executed, we can run the delayed children. The children receive a vector of rows.
- The results are grouped by some key so we can retreive them given a parent row
- Each query returns a Result which both generates the select statement and parses the result. They have access to all results, keyed by nested identifiers. `nested` generates a `Result` which retrieves the relevant child rows and parses them using these keys


We can extend this to monadic profunctors. Each QueryM also is a serializer to turn values into rows,  we diff rows to generate patches, and we apply patches to the database as insert/update/delete statements.
