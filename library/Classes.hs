{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE OverloadedLabels #-}

{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# OPTIONS_GHC -Wno-deprecations #-}
module Classes where
import Debug.Trace
import Types (QueryBuilder)
import GHC.Stack (HasCallStack)
import Database.HDBC.Sqlite3 (connectSqlite3)
import qualified Data.Vector as V
import Data.String (fromString)
import qualified Data.Vector.Mutable as VM
import qualified Data.Map as M
import Data.Typeable
import Control.Monad.ST (runST)
import Control.Monad.State.Strict
import Data.Monoid (Endo(..))
import qualified Data.Foldable as F
import Control.Monad.Reader
import Data.Maybe (fromJust)

import Database.Relational.OverloadedInstances ()
import Entity.Customer
import Entity.Account
import qualified Database.Relational.Monad.BaseType
import Data.Functor.Const
import Control.Monad.Identity
import qualified Database.Relational.SqlSyntax as SQL
import GHC.Generics (type (:*:) (..))
import qualified Database.Relational.Monad.Trans.Ordering as SQL
import qualified Database.Relational.Monad.Trans.Restricting as SQL
import qualified Database.Relational.Monad.Trans.Join as SQL
import qualified Database.Relational.Monad.Trans.Aggregating as SQL
import qualified Database.Relational.Monad.Simple as SQL
import qualified Database.Custom.IBMDB2 as SQL
import qualified Database.Relational.Monad.Simple as SQL
import qualified Database.Relational.Record as Record
import qualified Database.Relational.Type as Rel
import Control.Monad.Writer.Strict
import Database.Relational.Monad.Class

import qualified Database.HDBC.SqlValue as SQLV
import qualified Database.HDBC.Record as SQLV
import qualified Database.HDBC.Types as HDBC
import qualified Database.Record.FromSql as SQLV
import qualified Database.Record.Persistable as SQLP
import Data.Containers.ListUtils (nubOrd)
import Language.SQL.Keyword.Type (Keyword(..))

import GHC.Records
import qualified Database.Relational.OverloadedProjection as SQLOP

import Data.Functor.Identity
import qualified Entity.Entities
import GHC.Types (Symbol, Type)

testQ = asRoot $ do
    acc <- SQL.query account
    custs <- mkJoin acc.customer $ \customer ->
       pure (sel customer)
    SQL.wheres $ acc.availBalance SQL..>. SQL.value (Just (25000::Double))
    pure $ do
        ac <- sel acc
        cust <- custs
        pure (ac, cust)

class FundepHack a b c | a b -> c
instance FundepHack a b c => FundepHack a b c
class (FundepHack l b c) => ORMField (l::Symbol) b c where
    ormField :: Record.Record SQL.Flat b -> c
instance (FundepHack l b c, SQLP.PersistableWidth a, SQLOP.HasProjection l a b) => ORMField l a (Record.Record SQL.Flat b) where
    ormField r = r SQL.! SQLOP.projection @l undefined

instance (x ~ (JoinConfig Int Customer Singular)) => ORMField "customer" Account x where
    ormField r = JoinConfig {joinKeyR = #custId, joinTarget = customer, joinFinalizer = singular, joinOrigin = r.custId  }

instance (AccountField l b, FundepHack l Account b, ORMField l Account b) => HasField l (Record.Record SQL.Flat Account) b where
    getField r = ormField @l r
class IsProj b
instance (b ~ Record.Record SQL.Flat x) => IsProj b
class IsRel b
instance (b ~ JoinConfig x y z) => IsRel b
type family AccountField (l :: Symbol) b where
   AccountField "customer"  b = IsRel b
   AccountField l b = IsProj b

-- instance (x ~ (JoinConfig Int Singular)) => ORMField "customer" Account x where
--     ormField r = JoinConfig {joinKeyR = #custId, joinTarget = customer, joinFinalizer = singular, joinOrigin = r.custId  }
    
    


type QueryT = SQL.Orderings SQL.Flat SQL.QueryCore
type AggQueryT = SQL.Orderings SQL.Aggregated (SQL.Restrictings SQL.Aggregated (SQL.AggregatingSetT SQL.QueryCore))

toSubQuery :: (x -> SQL.Tuple) -> QueryT (x, Result a)        -- ^ 'SimpleQuery'' to run
           -> SQL.ConfigureQuery (x, SQL.SubQuery, RowParser a) -- ^ Result 'SubQuery' with 'Qualify' computation
toSubQuery toTups q = do
   (((((x, res), ot), rs), pd), da) <- (extract q)
   
   c <- SQL.askConfig
   let (pj, parser) = runResult res
       tups = toTups x
       fullTups = (tups <> pj)
       -- mapping = M.fromList (zip fullTups [1..])
   pure $ (x, SQL.flatSubQuery c fullTups da pd (map Record.untype rs) ot, (skipParser (length tups) *> parser))
  where
    extract =  SQL.extractCore . SQL.extractOrderingTerms

data QueryState = QS { qidGen :: Int, subQueries :: M.Map QId Query, selects :: [SQL.Column] }
instance Show QueryState where
   show QS{..} = "QS { qidGen = " <> show qidGen <> ", subQueries = ..., selects = " <> show selects <> " }"

    
type family TEval f a :: Type
data Singular
type instance TEval Singular a = a
data JoinConfig k b f
   = JoinConfig {
       joinKeyR :: SQL.Pi b k,
       joinTarget :: SQL.Relation () b,
       joinFinalizer :: forall x. [x] -> TEval f x,
       joinOrigin :: Record.Record SQL.Flat k
   }

joinConfig = JoinConfig undefined undefined
mkJoin :: (
    SQLV.FromSql HDBC.SqlValue k,
    SQLP.PersistableWidth k,
    SQLP.PersistableWidth b,
    SQL.LiteralSQL k,
    Typeable k,
    Typeable r,
    Ord k
  ) => JoinConfig k b f -> (Record.Record SQL.Flat b -> QueryM SQL.Flat (Result r)) ->  QueryM SQL.Flat (Result (TEval f r))
mkJoin cfg parse = do
    fmap (fmap (joinFinalizer cfg)) $ 
      nested (sel (joinOrigin cfg)) $ \row -> do
        child <- SQL.query (joinTarget cfg)
        let key = child SQL.!  joinKeyR cfg
        SQL.wheres $ SQL.in' key (SQL.values  row)
        childKey <- resultToRowParser (sel key)
        fmap (childKey,) (parse child)

-- M.Map PId (M.Map CId Row)
type UnParse r = ReaderT r (Const [(SQL.Column, SQLV.SqlValue)])

class QueryOut m where
    qsel :: (HasCallStack, SQLV.FromSql SQLV.SqlValue a, SQLP.PersistableWidth a) => Record.Record c a -> m a
    qref :: Result a -> m a
instance QueryOut Result where
    qsel = sel
    qref = id
class ToSqlList a where
    toSqlList :: a -> [SQLV.SqlValue]
instance ToSqlList r => QueryOut (UnParse r) where
    qsel r = ReaderT $ \s -> Const (zip (Record.untype r) (toSqlList s))

class ExecInsert r where
    execInsert :: r -> [(SQL.Column, SQLV.SqlValue)] -> IO Int
nested :: (Typeable a, Typeable k, Ord k) => (Result k) -> ([k] -> QueryM SQL.Flat (RowParser k, Result a)) -> QueryM m (Result [a])
nested parentKey cb = do
   qid <- genQId
   rp <- resultToRowParser parentKey
   tellQuery (Query qid cb rp)
   s <- get
   -- traceM (show s)
   pure $ getFetched (QKey qid) rp
genQId :: QueryM m QId
genQId = do
   qs <- get
   put qs { qidGen = qidGen qs+ 1 }
   pure $ QId (qidGen qs)
tellQuery :: Query -> QueryM m ()
tellQuery q = do
   modify $ \qs -> qs { subQueries = M.insert (queryId q) q (subQueries qs) }
resultToRowParser :: (Typeable a) => Result a -> QueryM m (RowParser a)
resultToRowParser (Result (Const endo :*:  rp)) = do
    offs <- tellSelect (appEndo endo [])
    pure (parseAtOffset offs rp)
parseAtOffset :: Int -> RowParser a -> RowParser a
parseAtOffset offs rp = do
    old <- get
    put offs
    out <- rp
    put old
    pure out

tellSelect :: [SQL.Column] -> QueryM m Int
tellSelect cols = do
   qs <- get
   let offs = length (selects qs)
   put qs { selects = selects qs <> cols }
   pure offs

sel :: forall a c. (HasCallStack, SQLV.FromSql SQLV.SqlValue a, SQLP.PersistableWidth a) => SQL.Record c a -> (Result a)
sel rec 
  | wid /= length (Record.untype rec) = error "sel: too many columns"
  | otherwise
    = Result $ (:*:) (Const $ Endo $ (Record.untype rec <>)) $ do
       (_, v) <- ask
       off <- get
       put (off + wid)
       pure $ r off v
  where
    wid = SQLP.runPersistableRecordWidth (SQLP.persistableWidth @a)
    sliceVector :: Int -> Int -> V.Vector SQLV.SqlValue -> V.Vector SQLV.SqlValue
    sliceVector start len  v
      | start < 0 || V.length v < start + len = error $ " Invalid slice " <> show (start,len,V.length v, show v, show (Record.untype rec))
      | otherwise = V.slice start len v
    r off v = SQLV.toRecord $ V.toList (sliceVector off wid v)
-- nested :: (V.Vector Row  -> QueryM (Result b)) -> QKey [b]
-- localResult :: Result a -> QueryM f (RowParser a)
newtype QueryM f a = QueryM { unQueryM :: StateT QueryState (QueryT) a }
  deriving (Functor, Applicative, Monad, MonadState QueryState)
instance MonadRestrict SQL.Flat (QueryM SQL.Flat) where
   restrict p = QueryM (lift $ SQL.restrict p)
instance  MonadQualify SQL.ConfigureQuery (QueryM f) where
   liftQualify p = QueryM (lift $ liftQualify p)
instance  MonadQuery (QueryM f) where
   setDuplication = QueryM . lift . setDuplication
   restrictJoin = QueryM . lift . restrictJoin
   query' = QueryM . lift . query'
   queryMaybe' = QueryM . lift . queryMaybe'
-- instance  MonadPartition f (QueryM f) where
--     partitionBy = QueryM . lift . partitionBy

type M a = StateT QueryState (QueryT) (a)
runQueryM :: Int -> QueryM SQL.Flat (x, Result a) -> (QueryState, SQL.SubQuery, (x, RowParser a))
runQueryM id0 (QueryM q) = undoShift $ SQL.configureQuery (toSubQuery (selects . snd) $ fmap (\((v,a),b) -> ((v,b),a)) $ runStateT q (QS id0 mempty mempty)) SQL.defaultConfig 
   where
     undoShift ((v,qs),sq,o) = (qs,sq,(v,o))

-- runTestQ :: IO ([Int])
runTestQ = do
    conn <- connectSqlite3 "examples.db"
    (_,a) <- runReaderT (runAQuery (pure ()) testQ) conn
    pure a

singular :: [a] -> a
singular [a] = a
singular _ = error "Invalid query"

-- testQ :: HasCallStack => QueryM SQL.Flat (RowParser (), Result (Account, ()))
       -- ac <- sel acc

      -- aid <- sel acc.accountId
      -- abal <- sel acc.availBalance
      -- pure (aid,abal)

asRoot = fmap (pure() ,)

runAQuery :: (Typeable k, Ord k, Typeable a, ExecQuery m) => RowParser k -> QueryM SQL.Flat (RowParser k, Result a) -> m (QMap, [a])
runAQuery rp q = do
   qmap <- loadNested $ Query (QId 0) (const q) rp
   pure $ (qmap, runParserRoot  qmap)

liftConfig :: SQL.ConfigureQuery a -> M a
liftConfig = lift . lift . lift . lift
  -- pure $ \r -> parser <$> SQL.nested r sub


type Row = V.Vector SQLV.SqlValue

class Monad m => GetRows m where
    getRows :: m QMap

class RestrictQuery r where
    restrictToParent :: r -> V.Vector Row -> QueryT ()
type Id = Int
class (Typeable k, Ord k) => RelationAnchor r k | r -> k where
    parseParentKey :: r -> Result k
    parseSelfKey :: r -> Result k
data QueryRoot = QueryRoot
  deriving (Eq, Ord, Show)
instance RelationAnchor QueryRoot () where
    parseParentKey _ = pure ()
    parseSelfKey _ = pure ()
-- class LoadRelation r k a | r -> k a where
--     parseResult :: r -> k -> m a

-- data ChildRef a = ChildRef { childRef :: a, childKey :: Key a }

newtype QId = QId Int deriving (Eq, Ord, Show)
class MakeQuery a where

data NJoin a b = With a b


data Query = forall k a. (Ord k, Typeable k, Typeable a) => Query {
    queryId :: !QId,
    sqlQuery :: [k] -> QueryM SQL.Flat (RowParser k, Result a),
    parentKey :: RowParser k
}

skipParser :: Int -> RowParser ()
skipParser i = modify (+i)
newtype RowParser a = RowParser { unRowParser :: ReaderT (QMap, V.Vector SQLV.SqlValue) (State Int) a }
  deriving (Functor, Applicative, Monad, MonadReader (QMap, V.Vector SQLV.SqlValue), MonadState Int)

type KeyParser a = RowParser a
runRowParser :: RowParser a -> QMap -> V.Vector SQLV.SqlValue ->  a
runRowParser (RowParser p) qmap v  = evalState (runReaderT p (qmap, v)) 0
runKeyParser :: KeyParser a -> V.Vector SQLV.SqlValue -> a
runKeyParser rp v  = runRowParser rp undefined v

newtype Result a = Result { unResult :: (Const (Endo SQL.Tuple) :*: RowParser) a }
  deriving (Functor, Applicative)
runResult :: Result a -> (SQL.Tuple, RowParser a)
runResult (Result (Const e :*: p)) = (appEndo e mempty, p)
type Parser a = Reader ParserState a



instance SQLV.FromSql SQLV.SqlValue Row where
    recordFromSql = SQLV.createRecordFromSql $ \v -> (V.fromList v, [])
    
class Monad m => ExecQuery m where
    execQuery :: SQL.SubQuery -> m [Row]
instance (HDBC.IConnection conn) => ExecQuery (ReaderT conn IO) where
    execQuery q = do
        conn <- ask
        liftIO $ SQLV.runQuery conn query ()
      where
        rel :: SQL.Relation () Row
        rel = SQL.unsafeTypeRelation (pure q)
        query :: Rel.Query () Row
        query = Rel.relationalQuery_ SQL.defaultConfig{SQL.schemaNameMode = SQL.SchemaNotQualified} rel  []
instance (Monad m, ExecQuery m) => ExecQuery (StateT s m) where
    execQuery = lift . execQuery

mkGrouping :: (Ord k) => RowParser k -> [Row] -> M.Map k (V.Vector Row)
mkGrouping kp rows = M.map (V.fromList . flip appEndo []) $ M.fromListWith (<>) [(runKeyParser kp row, Endo (row:))| row <- rows]


data ResultMap = forall k a. (Typeable k, Ord k, Typeable a) => ResultMap (M.Map k (V.Vector Row)) (RowParser a)
unResultMap :: (Typeable k, Typeable a) => ResultMap -> Maybe (M.Map k (V.Vector Row), RowParser a)
unResultMap (ResultMap m b) = cast (m, b)
unResultMap1 :: (Typeable k) => ResultMap -> Maybe (M.Map k (V.Vector Row))
unResultMap1 (ResultMap m b) = cast (m)

newtype QMap = QMap { unQMap :: M.Map QId ResultMap}


data HasMany = HasMany { parentId :: QId, selfId :: QId, parentCol :: String, childCol :: String, table :: String }
  deriving (Eq, Ord, Show, Typeable)
data Selector = QId :. String
selectField :: Selector -> QueryBuilder -> QueryBuilder
selectField = undefined
instance RelationAnchor HasMany String where
    -- parseParentKey r row = row M.! parentCol r
    -- parseSelfKey r row = row M.! childCol r
instance RestrictQuery HasMany where

lookupQMapRoot :: QMap -> V.Vector Row
lookupQMapRoot qmap = do
   case (unQMap qmap) M.! QId 0 of
     ResultMap p _ | Just o <- cast p -> o M.! ()
     _ -> error "Illegal root parser for qid 0"
lookupQMapParser :: forall a. (Typeable a) => QKey a -> QMap -> RowParser a
lookupQMapParser (QKey qid) qmap = do
   case (unQMap qmap) M.! qid of
     ResultMap _ p | Just o <- cast p -> o
     p -> error ("Illegal parer" <> show qid <> ", expected type " <> show (typeOf (undefined :: a)) <> ", got type " <> showRMTyp p)
    where
        showRMTyp (ResultMap @b _ _) = show (typeOf (undefined :: b))
lookupQMap :: (Typeable k, Ord k) => QId -> k -> QMap -> Maybe (V.Vector Row)
lookupQMap qid k qmap = do
   mrm <- M.lookup qid (unQMap qmap)
   rm <- unResultMap1 mrm
   M.lookup k rm
data QKey a = QKey {theId :: QId}
data ParserState = ParserState {
    pQuery :: Query,
    curData :: QMap,
    curRow :: Row
}

getCurRow :: (MonadReader ParserState m) => m Row
getCurRow = do
    ParserState {curRow} <- ask
    pure curRow

castParser :: forall a b. (Typeable a, Typeable b) => Reader ParserState a -> Maybe (Reader ParserState b)
castParser r
  | Just Refl <- eqT @a @b = Just r
  | otherwise = Nothing
-- qrel :: QKey a -> RowParser a
-- qrel QKey {theParser=parser} = parser

loadNested :: (Monad m, ExecQuery m) => Query -> m QMap
loadNested  q0 = QMap <$> evalStateT (go (V.fromList []) q0) 1
  where
    go v (Query qId q' kp) = do
      idx <- get
      let parsedKeys = map (runKeyParser kp) (V.toList v)
      (QS {qidGen = idx', subQueries=m', selects=cols},sql,(keyParser, parser)) <- pure (runQueryM idx (q' parsedKeys))
      put idx'
      out <- execQuery sql
      let v' = V.fromList out
      children <- traverse (go v') (M.elems m')
      return $ M.insert qId (ResultMap (mkGrouping keyParser (V.toList v')) parser) (mconcat children)


getFetched :: (Typeable a, Typeable k, Ord k) => QKey a -> KeyParser k -> Result [a]
getFetched qk kp = Result $ (Const mempty :*:) $ do
    k <- kp
    (qm, _) <- ask
    pure $ runParserFor qk k qm
   
runParserRoot :: Typeable a => QMap -> [a]
runParserRoot qmap = runParserFor (QKey (QId 0)) () qmap

runParserFor :: (Typeable a, Typeable k, Ord k) => QKey a -> k -> QMap -> [a]
runParserFor qkey k qmap = map (runRowParser pars qmap) (V.toList root)
  where
    pars = lookupQMapParser qkey qmap
    root = fromJust (lookupQMap (theId qkey) k qmap)


pList :: Reader ParserState a -> [Row] -> [a]
pList = undefined
getField :: String -> Row -> a
getField = undefined

