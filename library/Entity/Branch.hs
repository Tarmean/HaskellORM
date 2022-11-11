{-# LANGUAGE QuasiQuotes, TemplateHaskell, MultiParamTypeClasses, FlexibleInstances, DeriveGeneric, DataKinds #-}
{-# LANGUAGE NoFieldSelectors #-}
module Entity.Branch where
import TemplateHaskellDB(defineTable)
$(defineTable "examples.db" "branch")

