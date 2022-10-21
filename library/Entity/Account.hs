{-# LANGUAGE QuasiQuotes, TemplateHaskell, MultiParamTypeClasses, FlexibleInstances, DeriveGeneric, DataKinds #-}
{-# LANGUAGE NoFieldSelectors #-}
module Entity.Account where
import TemplateHaskellDB(defineTable)
$(defineTable "examples.db" "account")
