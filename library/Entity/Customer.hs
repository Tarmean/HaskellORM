{-# LANGUAGE QuasiQuotes, TemplateHaskell, MultiParamTypeClasses, FlexibleInstances, DeriveGeneric, DataKinds #-}
{-# OPTIONS_GHC -ddump-splices #-}

module Entity.Customer where
import TemplateHaskellDB(defineTable)
$(defineTable "examples.db" "customer")

