{-# LANGUAGE QuasiQuotes, TemplateHaskell, MultiParamTypeClasses, FlexibleInstances, DeriveGeneric, DataKinds #-}
module Entity.Customer where
import TemplateHaskellDB(defineTable)
$(defineTable "examples.db" "customer")

