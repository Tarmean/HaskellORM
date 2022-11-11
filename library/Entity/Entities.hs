{-# LANGUAGE QuasiQuotes, TemplateHaskell, MultiParamTypeClasses, FlexibleInstances, DeriveGeneric, DataKinds #-}
{-# LANGUAGE NoFieldSelectors #-}
module Entity.Entities where
import TemplateHaskellDB(defineTable)
$(defineTable "examples.db" "business")
-- $(defineTable "examples.db" "account")
-- $(defineTable "examples.db" "customer")
-- $(defineTable "examples.db" "department")
-- $(defineTable "examples.db" "employee")
-- $(defineTable "examples.db" "individual")
-- $(defineTable "examples.db" "officer")
-- $(defineTable "examples.db" "product")
-- $(defineTable "examples.db" "product_type")

-- $(defineTable "examples.db" "transaction0")

-- type Transaction = Transaction0

-- transaction :: Relation () Transaction
-- transaction = transaction0

