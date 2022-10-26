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
{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DerivingStrategies #-}
module Classes where
import GHC.Stack (HasCallStack)
import Database.HDBC.Sqlite3 (connectSqlite3)
import qualified Data.Vector as V
import Data.String ()
import qualified Data.Map as M
import Data.Typeable
import Control.Monad.State.Strict
import Control.Monad.Reader
import Data.Maybe (fromJust)

import Database.Relational.OverloadedInstances ()
import Entity.Customer
import Entity.Account
import qualified Database.Relational.SqlSyntax as SQL
import qualified Database.Relational.Monad.Trans.Ordering as SQL
import qualified Database.Relational.Monad.Trans.Restricting as SQL
import qualified Database.Relational.Monad.Trans.Aggregating as SQL
import qualified Database.Custom.IBMDB2 as SQL
import qualified Database.Relational.Record as Record
import qualified Database.Relational.Type as Rel
import Control.Monad.Writer.Strict hiding (Ap)
import Database.Relational.Monad.Class

import qualified Database.HDBC.SqlValue as SQLV
import qualified Database.HDBC.Record as SQLV
import qualified Database.HDBC.Types as HDBC
import qualified Database.Record.FromSql as SQLV
import qualified Database.Record.Persistable as SQLP
import FreeAp

import GHC.Records
import qualified Database.Relational.OverloadedProjection as SQLOP

import GHC.Types (Symbol, Type)
import Data.Dynamic
import qualified Database.Record.ToSql as Record

data UpdateStep = UpsertNow (IO [(SQL.Column, SQLV.SqlValue)]) | DeleteLater (IO ())

data Updater = Updater {
    runUpdate :: [(SQL.Column, SQLV.SqlValue)] -> [(SQL.Column, SQLV.SqlValue)] -> UpdateStep
    propagation :: [(SQL.Column, SQL.Column)]
}

-- TODO: implement PostQuery updates
-- it's another tree traversal
--
-- requires a merging operator
-- requires access to the old data
-- requires some grouping, but on the raw rows or on the parsed data?
data Query = forall k a. (Ord k, Typeable k, Typeable a) => Query {
    queryId :: !QId,
    sqlQuery :: [k] -> QueryM SQL.Flat (RowParser k, Result a),
    parentKey :: RowParser k
}
data QueryKind = KeyInParent | KeyInChild
kpInsert :: Row -> () -> IO Row
kpInsert = undefined
kpUpdate :: Row -> Row -> IO Row
kpUpdate = undefined
kpDelete :: Row -> Row -> IO Row
kpDelete = undefined

kcInsert :: Row -> () -> IO ()
kcInsert = undefined
kcUpdate :: Row -> Row -> IO ()
kcUpdate = undefined
kcDelete :: Row -> Row -> IO ()
kcDelete = undefined

-- kp:
--   onInsert:
--     - insert: pre
--     - update: _
--     - delete: n/a
--   onUpdate:
--     - insert: pre
--     - update: _
--     - delete: pre
--   onDelete:
--     - insert: n/a
--     - update: n/a
--     - delete: post
--
-- kc:
--   onInsert:
--     - insert: post
--     - update: post
--     - delete: n/a
--   onUpdate:
--    - insert: _
--    - update: _
--    - delete: _
--   onDelete:
--    - insert: n/a
--    - update: n/a
--    - delete: pre


-- runDelete :: Typeable a => PostQuery -> a -> IO ()
-- runDelete pq a = do
--    traverse_ (uncurry (runDeletePre rs)) (M.intersectWith (,) childrenQueries vm)
--    execDelete rs
--    traverse_ (uncurry (runDeletePost rs)) (M.intersectWith (,) childrenQueries vm)
--   where
--     (rs, VMap vm) = unparseQuery pq a
-- runUpsert :: Typeable a => PostQuery -> a -> IO [(SQL.Column, SQLV.SqlValue)]
-- runUpsert pq a =  do
--    os <- traverse (uncurry (runInnerPost rs)) (M.intersectWith (,) childrenQueries vm)
--    let rs' = rs <> os
--    o <- execUpsert (rs <> os)
--    let rs'' = rs' <> o
--    traverse_ (uncurry (runInnerPost rs'')) (M.intersectWith (,) childrenQueries vm)
--   where
--     (rs, VMap vm) = unparseQuery pq a

data PostQuery = forall k a. (Ord k, Typeable k, Typeable a) => PostQuery {
    pqueryId :: !QId,
    unparseQuery :: a -> ([(SQL.Column, SQLV.SqlValue)], VMap),
    childrenQueries :: M.Map QId Query
}


testQ :: QueryM
  SQL.Flat
  (RowParser (),
   Ap ResultF (Account, [Customer]) (Account, [Customer]))
testQ = asRoot $ do
    accQ <- SQL.query account
    custQ <- mkJoin accQ.customer $ \cust ->
       pure (sel cust)
    SQL.wheres $ accQ.availBalance SQL..>. SQL.value (Just (25000::Double))
    pure $ do
        ac <- fst =. sel accQ
        cust <- (\(acc, cust) -> (acc.custId, cust)) =. custQ
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



type QueryT = SQL.Orderings SQL.Flat SQL.QueryCore
type AggQueryT = SQL.Orderings SQL.Aggregated (SQL.Restrictings SQL.Aggregated (SQL.AggregatingSetT SQL.QueryCore))

toSubQuery :: (x -> SQL.Tuple) -> QueryT (x, Result a)        -- ^ 'SimpleQuery'' to run
           -> SQL.ConfigureQuery (x, SQL.SubQuery, RowParser a) -- ^ Result 'SubQuery' with 'Qualify' computation
toSubQuery toTups q = do
   (((((x, res), ot), rs), pd), da) <- (extract q)
   c <- SQL.askConfig
   let (pj, parser) = (interpTuple res, interpParser res)
       tups = toTups x
       fullTups = (tups <> pj)
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

mkJoin :: (
    SQLV.FromSql HDBC.SqlValue k,
    SQLP.PersistableWidth k,
    SQLP.PersistableWidth b,
    SQL.LiteralSQL k,
    Typeable k,
    Typeable r,
    Ord k, Record.ToSql HDBC.SqlValue k
  ) => JoinConfig k b f -> (Record.Record SQL.Flat b -> QueryM SQL.Flat (Result r)) ->  QueryM SQL.Flat (Result' (k, [r]) [r])
mkJoin cfg parse = do
      nested (sel (joinOrigin cfg)) $ \row -> do
        child <- SQL.query (joinTarget cfg)
        let key = child SQL.!  joinKeyR cfg
        SQL.wheres $ SQL.in' key (SQL.values  row)
        childKey <- resultToRowParser (sel' key)
        fmap (childKey,) (parse child)

type UnParse r = r -> [(SQL.Column, SQLV.SqlValue)]


nested :: (Typeable a, Typeable k, Ord k) => (Result k) -> ([k] -> QueryM SQL.Flat (RowParser k, Result a)) -> QueryM m (Result' (k, [a]) [a])
nested parentKey cb = do
   qid <- genQId
   rp <- resultToRowParser parentKey
   tellQuery (Query qid cb rp)
   pure $ getFetched (QKey qid) (interpPrinter parentKey) rp
genQId :: QueryM m QId
genQId = do
   qs <- get
   put qs { qidGen = qidGen qs+ 1 }
   pure $ QId (qidGen qs)
tellQuery :: Query -> QueryM m ()
tellQuery q = do
   modify $ \qs -> qs { subQueries = M.insert (queryId q) q (subQueries qs) }
resultToRowParser :: (Typeable a) => Result' x a -> QueryM m (RowParser a)
resultToRowParser r = do
    offs <- tellSelect (interpTuple r)
    pure (parseAtOffset offs (interpParser r))
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

sel :: forall a c. (HasCallStack, SQLV.FromSql SQLV.SqlValue a, SQLP.PersistableWidth a, Record.ToSql SQLV.SqlValue a) => SQL.Record c a -> (Result a)
sel r = Record.runFromRecord Record.recordToSql =. sel' r
sel' :: forall a c. (HasCallStack, SQLV.FromSql SQLV.SqlValue a, SQLP.PersistableWidth a) => SQL.Record c a -> (Result' [SQLV.SqlValue] a)
sel' rec 
  | wid /= length (Record.untype rec) = error "sel: too many columns"
  | otherwise = liftAp $ ParseQ (Record.untype rec) id $ do
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

type M a = StateT QueryState (QueryT) (a)
runQueryM :: Int -> QueryM SQL.Flat (x, Result a) -> (QueryState, SQL.SubQuery, (x, RowParser a))
runQueryM id0 (QueryM q) = undoShift $ SQL.configureQuery (toSubQuery (selects . snd) $ fmap (\((v,a),b) -> ((v,b),a)) $ runStateT q (QS id0 mempty mempty)) SQL.defaultConfig 
   where
     undoShift ((v,qs),sq,o) = (qs,sq,(v,o))

runTestQ :: IO [(Account, [Customer])]
runTestQ = do
    conn <- connectSqlite3 "examples.db"
    (_,a) <- runReaderT (runAQuery (pure ()) testQ) conn
    pure a

singular :: [a] -> a
singular [a] = a
singular _ = error "Invalid query"


asRoot :: QueryM SQL.Flat a -> QueryM SQL.Flat (RowParser (), a)
asRoot = fmap (pure() ,)

runAQuery :: (Typeable k, Ord k, Typeable a, ExecQuery m) => RowParser k -> QueryM SQL.Flat (RowParser k, Result a) -> m (QMap, [a])
runAQuery rp q = do
   qmap <- loadNested $ Query (QId 0) (const q) rp
   pure $ (qmap, runParserRoot  qmap)

liftConfig :: SQL.ConfigureQuery a -> M a
liftConfig = lift . lift . lift . lift


type Row = V.Vector SQLV.SqlValue


class RestrictQuery r where
    restrictToParent :: r -> V.Vector Row -> QueryT ()
type Id = Int

newtype QId = QId Int deriving (Eq, Ord, Show)




skipParser :: Int -> RowParser ()
skipParser i = modify (+i)
newtype RowParser a = RowParser { unRowParser :: ReaderT (QMap, V.Vector SQLV.SqlValue) (State Int) a }
  deriving (Functor, Applicative, Monad, MonadReader (QMap, V.Vector SQLV.SqlValue), MonadState Int)

type KeyParser a = RowParser a
runRowParser :: RowParser a -> QMap -> V.Vector SQLV.SqlValue ->  a
runRowParser (RowParser p) qmap v  = evalState (runReaderT p (qmap, v)) 0
runKeyParser :: KeyParser a -> V.Vector SQLV.SqlValue -> a
runKeyParser rp v  = runRowParser rp undefined v


type Result' = Ap ResultF
type Result a = Result' a a
data ResultF r a where
    NestedQ :: Typeable a => QKey a -> UnParse r -> RowParser [a] -> ResultF (r, [a]) [a]
    ParseQ :: { cols :: SQL.Tuple, colPrinter ::  (r -> [SQLV.SqlValue]) , colParser :: RowParser a } -> ResultF r a
interpParser :: Result' x a -> RowParser a
interpParser =  runAp \case
    NestedQ _ _ q -> q
    ParseQ _ _ q -> q
interpTuple :: Result' x a -> SQL.Tuple
interpTuple = runAp_ \case
    NestedQ _ _ _ -> []
    ParseQ t _ _  -> t
interpPrinter :: Result' r a -> r -> [(SQL.Column, SQLV.SqlValue)]
interpPrinter = runBAp_ $ \x -> \case
    NestedQ _ r _ -> r (fst x)
    ParseQ t prnt _ -> (zip t (prnt x))
interpChildren :: Result' a a -> a -> VMap
interpChildren = runBAp_ $ \x -> \case
    NestedQ v _ _ -> VMap (M.singleton (theId v) (toDyn $ snd x))
    ParseQ _ _ _ -> mempty

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
unResultMap1 :: (Typeable k) => ResultMap -> Maybe (M.Map k (V.Vector Row))
unResultMap1 (ResultMap m _) = cast m

newtype QMap = QMap { unQMap :: M.Map QId ResultMap}
newtype VMap = VMap { unVMap :: M.Map QId Dynamic }
instance Semigroup VMap where
    VMap a <> VMap b = VMap (M.unionWith (error "VMap collision") a b)
instance Monoid VMap where
    mempty = VMap mempty


data HasMany = HasMany { parentId :: QId, selfId :: QId, parentCol :: String, childCol :: String, table :: String }
  deriving (Eq, Ord, Show, Typeable)

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


loadNested :: (Monad m, ExecQuery m) => Query -> m QMap
loadNested  q0 = QMap <$> evalStateT (go (V.fromList []) q0) 1
  where
    go v (Query qId q' kp) = do
      idx <- get
      let parsedKeys = map (runKeyParser kp) (V.toList v)
      (QS {qidGen = idx', subQueries=m'},sql,(keyParser, parser)) <- pure (runQueryM idx (q' parsedKeys))
      put idx'
      out <- execQuery sql
      let v' = V.fromList out
      children <- traverse (go v') (M.elems m')
      return $ M.insert qId (ResultMap (mkGrouping keyParser (V.toList v')) parser) (mconcat children)


getFetched :: (Typeable a, Typeable k, Ord k) => QKey a -> UnParse k -> KeyParser k -> Result' (k, [a]) [a]
getFetched qk up kp = liftAp $ NestedQ qk up $ do
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

