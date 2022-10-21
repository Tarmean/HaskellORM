{-# LANGUAGE QuasiQuotes, TemplateHaskell, MultiParamTypeClasses, FlexibleInstances, DeriveGeneric, DataKinds #-}
module Entity.Individual where
import TemplateHaskellDB(defineTable)
$(defineTable "examples.db" "individual")

