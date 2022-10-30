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

import Database.Relational.OverloadedInstances ()
import Entity.Customer ( Customer(custId), customer )
import Entity.Account ( Account(accountId), account )
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
import FreeAp ( Ap, runAp, runAp_, runBAp, runBAp_, (=.), liftAp )
import qualified Data.Monoid as Monoid

import GHC.Records
import qualified Database.Relational.OverloadedProjection as SQLOP

import GHC.Types (Symbol, Type)
import Data.Dynamic
import qualified Database.Record.ToSql as Record
import Data.Functor.Identity (Identity(..))
import qualified Data.Set as S
import Control.Applicative (Applicative(liftA2))

data UpdateStep = UpsertNow (IO [(SQL.Column, SQLV.SqlValue)]) | DeleteLater (IO ())

data UpdatedRows = UR (M.Map Int UpdatedRow)
instance Semigroup UpdatedRows where
  UR a <> UR b = UR (M.unionWith (<>) a b)
instance Monoid UpdatedRows where
    mempty = UR M.empty
data UpdatedRow = DeleteRow | Noop | SetRow [(Int, SQLV.SqlValue)]
    deriving (Show)
instance Semigroup UpdatedRow where
    (<>) Noop x = x
    (<>) x Noop = x
    (<>) DeleteRow DeleteRow = DeleteRow
    (<>) (SetRow a) (SetRow b) = SetRow (a <> b)
    (<>) a b = error $ "UpdatedRow: " <> show a <> " <> " <> show b
instance Monoid UpdatedRow where
    mempty = Noop
type QueryRef = SQL.Qualified Int
data Updater = Updater {
    runUpdate :: Maybe [(QueryRef, SQLV.SqlValue)] -> UpdatedRow -> UpdateStep,
    propagation :: [(SQL.Column, SQL.Column)]
}

execUpdate :: forall a k. (Typeable a, Typeable k) => k -> [a] -> QKey k a -> QMap -> IO ()
execUpdate g a k qmap =  undefined
  where
    changes = deltaRows g k qmap a
    applyOne up (Just i, Nothing) = runUpdate up (Just i) DeleteRow
   -- ResultEntry @_ @k' @a' dat parse write
   --   | (Just Refl,Just Refl) <- (eqT @a @a', eqT @k @k') -> do
   --     let old = dat M.! g
   --     undefined
   --   | otherwise -> error "boo"


-- TODO: implement PostQuery updates
-- it's another tree traversal
--
-- requires a merging operator
-- requires access to the old data
-- requires some grouping, but on the raw rows or on the parsed data?
-- data Query = forall r a. (Typeable r, Ord r, Ord k, Typeable k, Typeable a) => Query {
--     queryId :: !QId,
-- }
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


testQ :: QueryM SQL.Flat (AResult Int (Account, [Customer]))
testQ = do
    accQ <- SQL.query account
    custQ <- mkJoin accQ.customer $ \aCust -> pure do
       cust_id <- (\c -> c.custId) =. sel aCust.custId
       c <- sel aCust
       pure (cust_id, c)
    SQL.wheres $ accQ.availBalance SQL..>. SQL.value (Just (25000::Double))
    pure $ do
        ac <- fst =. sel accQ
        cust <- snd =. custQ
        ac_id <- (\(a,_) -> a.accountId) =. sel accQ.accountId
        pure (ac_id, (ac, cust))

class FundepHack a b c | a b -> c
instance FundepHack a b c => FundepHack a b c
class (FundepHack l b c) => ORMField (l::Symbol) b c where
    ormField :: Record.Record SQL.Flat b -> c
instance (FundepHack l b c, SQLP.PersistableWidth a, SQLOP.HasProjection l a b) => ORMField l a (Record.Record SQL.Flat b) where
    ormField r = r SQL.! SQLOP.projection @l undefined

instance (x ~ (JoinConfig Int Customer Singular)) => ORMField "customer" Account x where
    ormField r = JoinConfig {joinKeyR = #custId, joinTarget = customer, joinFinalizer = singular, joinOrigin = r.custId  }

instance (CustomerField l b, FundepHack l Customer b, ORMField l Customer b) => HasField l (Record.Record SQL.Flat Customer) b where
    getField r = ormField @l r
instance (AccountField l b, FundepHack l Account b, ORMField l Account b) => HasField l (Record.Record SQL.Flat Account) b where
    getField r = ormField @l r
class IsProj b
instance (b ~ Record.Record SQL.Flat x) => IsProj b
class IsRel b
instance (b ~ JoinConfig x y z) => IsRel b
type family AccountField (l :: Symbol) b where
   AccountField "customer"  b = IsRel b
   AccountField l b = IsProj b
type family CustomerField (l :: Symbol) b where
   -- CustomerField "customer"  b = IsRel b
   CustomerField l b = IsProj b



type QueryT = SQL.Orderings SQL.Flat SQL.QueryCore
type AggQueryT = SQL.Orderings SQL.Aggregated (SQL.Restrictings SQL.Aggregated (SQL.AggregatingSetT SQL.QueryCore))

toSubQuery :: QueryT (x, Result' n a)        -- ^ 'SimpleQuery'' to run
           -> SQL.ConfigureQuery (x, SQL.SubQuery, Result' n a) -- ^ Result 'SubQuery' with 'Qualify' computation
toSubQuery q = do
   (((((x, res), ot), rs), pd), da) <- (extract q)
   c <- SQL.askConfig
   let tups = interpTuple res
   pure $ (x, SQL.flatSubQuery c tups da pd (map Record.untype rs) ot, res)
  where
    extract =  SQL.extractCore . SQL.extractOrderingTerms

data QueryState = QS { qidGen :: Int, selects :: [SQL.Column], updaters :: [Updater] }
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
    Show k, 
    Typeable k,
    Typeable r,
    Typeable fd,
    Ord fd,
    Ord k, Record.ToSql HDBC.SqlValue k
  ) => JoinConfig k b f -> (Record.Record SQL.Flat b -> QueryM SQL.Flat (Result' r (fd, r))) ->  QueryM SQL.Flat (Result' [r] [r])
mkJoin cfg parse = do
      nested (sel (joinOrigin cfg)) $ \row -> do
        child <- SQL.query (joinTarget cfg)
        let key = child SQL.!  joinKeyR cfg
        SQL.wheres $ SQL.in' key (SQL.values row)
        res <- parse child
        pure $ liftA2 (,) (undefined =. sel key) res

type UnParse r = r -> [(SQL.Column, SQLV.SqlValue)]

nested :: (Show k, Typeable a, Typeable k, Ord k, Typeable r, Ord r) => (Result k) -> ([k] -> QueryM SQL.Flat (Result' a (k, (r, a)))) -> QueryM m (Result' [a] [a])
nested parentKey cb = do
   qid <- genQId
   pure $ liftAp $ NestedQ {
        keyParser=parentKey,
        nestedKey = QKey qid,
        nestedQuery = cb
        }

genQId :: QueryM m QId
genQId = do
   qs <- get
   put qs { qidGen = qidGen qs+ 1 }
   pure $ QId (qidGen qs)
sel :: forall a c. (HasCallStack, SQLV.FromSql SQLV.SqlValue a, SQLP.PersistableWidth a, Record.ToSql HDBC.SqlValue a) => SQL.Record c a -> (Result' a a)
sel rec 
  | wid /= length (Record.untype rec) = error "sel: too many columns"
  | otherwise = liftAp $ ParseQ (Record.untype rec) decRows $ do
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
    decRows a = decideUpdatesDefault (Record.untype rec) (Record.runFromRecord Record.recordToSql a :: [SQLV.SqlValue])

-- >>> :i SQL.Column
-- type Column :: *
-- data Column
--   = RawColumn StringSQL
--   | SubQueryRef (Qualified Int)
--   | Scalar SubQuery
--   | Case CaseClause Int
--   	-- Defined in ‘Database.Relational.SqlSyntax.Types’
-- instance Show Column
--   -- Defined in ‘Database.Relational.SqlSyntax.Types’
-- instance Monad m => MonadPartition c (PartitioningSetT c m)
--   -- Defined in ‘Database.Relational.Monad.Trans.Aggregating’

decideUpdatesDefault :: SQL.Tuple -> [HDBC.SqlValue] -> UpdatedRows
decideUpdatesDefault tups vals = UR $ M.fromListWith (<>) [(idx, SetRow [(argPos, val)])  | (SQL.SubQueryRef (SQL.Qualified (SQL.Qualifier idx) argPos), val) <- zip tups vals ]
  -- [] -> mempty
  -- xs -> SetRow xs
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
runQueryM :: forall n a. Int -> QueryM SQL.Flat (Result' n a) -> (QueryState, SQL.SubQuery, (Result' n a))
runQueryM id0 (QueryM q) = out 
  where
    ms = runStateT q (QS id0 mempty [])
    ns = do
      (res,queryState) <- ms
      pure (queryState,res)
    out = SQL.configureQuery (toSubQuery ns) SQL.defaultConfig

runTestQ :: IO [(Account, [Customer])]
runTestQ = do
    conn <- connectSqlite3 "examples.db"
    (_,a) <- runReaderT (runAQuery testQ) conn
    pure a

singular :: [a] -> a
singular [a] = a
singular _ = error "Invalid query"

asRoot :: QueryM SQL.Flat (Result' r a) -> QueryM SQL.Flat (Result' r ((), a))
asRoot = fmap (fmap ((),))

runAQuery :: (Typeable a, ExecQuery m, Typeable r, Ord r) => QueryM SQL.Flat (Result' a (r, a)) -> m (QMap, [a])
runAQuery q = do
   qmap <- loadNested (asRoot q)
   pure $ (qmap, runParserRoot qmap)

liftConfig :: SQL.ConfigureQuery a -> M a
liftConfig = lift . lift . lift . lift

type Row = V.Vector SQLV.SqlValue
type Row' = [(SQL.Column, SQLV.SqlValue)]

type Id = Int

newtype QId = QId Int deriving (Eq, Ord, Show)

newtype RowParser a = RowParser { unRowParser :: ReaderT (QMap, V.Vector SQLV.SqlValue) (State Int) a }
  deriving (Functor, Applicative, Monad, MonadReader (QMap, V.Vector SQLV.SqlValue), MonadState Int)

type KeyParser a = RowParser a
runRowParser :: RowParser a -> QMap -> V.Vector SQLV.SqlValue ->  a
runRowParser (RowParser p) qmap v  = evalState (runReaderT p (qmap, v)) 0
runKeyParser :: KeyParser a -> V.Vector SQLV.SqlValue -> a
runKeyParser rp v  = runRowParser rp undefined v


type AResult k a = Result' a (k, a)
type Result a = Result' a a

type Result' = Ap ResultF
-- | The QueryBuilder produces a Result, which is a binary tree of ResultF's.
-- We treat sql as a render=>mutate=>apply diffs loop similar to bidirectional transformations and reactive programming.
-- The result acts as
-- - The select statement for the flat query, i.e. a list of sql expressions
-- - A set of nested queries which are loaded breadth-first in batches, one query per nested join
-- - A parser to turn rows to values, invoking recursive parsers for nested values
--
-- After we updated the values we re-use the parser in the other direction:
-- - A serializer to turn values to rows and nested lists
-- - Updaters to diff old/new rows and generate update values in the database
-- - For each nested query, a fundep to align the nested old/new values and a recursive updater
--
-- That's a lot! We use a free applicative which we then re-interpret a bunch of times.
-- To store intermediate results from the batched loading we use a typeable map keyed by QKey's.
-- The NestedQ leaves must contain the corresponding typeable instances.
data ResultF x a where
     -- | A nested query.
     -- `a` is the result
     -- `k` is the key joined to the parent. It is not a unique key, otherwise we could use a non-nested join
     -- `r` is a grouping key. parent+r is a fundep which uniquely identifies
     -- the row. Used to lookup the corresponding old row when diffing updated
     -- values, if one exists
    NestedQ :: (Typeable r, Ord r, Show k, Ord k, Typeable k, Typeable a) => 
      {  keyParser :: Ap ResultF k k, -- ^ lookup join key for the parent
         nestedKey :: QKey k a,
         nestedQuery :: [k] -> QueryM SQL.Flat (Ap ResultF a (k, (r, a)))
      } -> ResultF [a] [a]
    ParseQ :: { cols :: SQL.Tuple, colPrinter ::  (a -> UpdatedRows) , colParser :: RowParser a } -> ResultF a a
data DepSet = forall s. (Typeable s, Ord s, Show s) => DepSet { unDepSet :: S.Set s }
instance Show DepSet where
  show (DepSet @s s) = show s <> " :: DepSet " <> show (typeRep (Proxy @s))
getDeps :: forall k. Typeable k => DepSet -> [k]
getDeps (DepSet s) = maybe (error "Type Mismatch") S.toList $ cast @_ @(S.Set k) s
newtype DepMap = DepMap {unDepMap :: M.Map QId DepSet}
 deriving Show
instance Semigroup DepMap where
    (DepMap m1) <> (DepMap m2) = DepMap (M.unionWith (<>) m1 m2)
instance Monoid DepMap where
    mempty = DepMap mempty
getDep :: Typeable k => QKey k a -> DepMap -> [k]
getDep (QKey qid) (DepMap m) = maybe [] getDeps $ M.lookup qid m
instance Semigroup DepSet where
    DepSet @s a <> DepSet @t b = case eqT @s @t of
       Just Refl -> DepSet (S.union a b)
       Nothing -> error "Invalid DepSet"

interpJoins :: Row -> Result' x a -> M.Map QId DepSet
interpJoins r res = unDepMap $ runKeyParser (Monoid.getAp $ runAp_ (\case
    NestedQ {keyParser = p, nestedKey = qid} -> Monoid.Ap $ (\o -> DepMap (M.singleton (theId qid) (DepSet $ S.singleton $ o))) <$> interpParser p
    ParseQ {colParser=keyParser} ->  Monoid.Ap (mempty <$ keyParser)) res) r

data SomeQuery = forall k a r.  (Typeable k, Typeable r, Typeable a) => SomeQuery ([k] -> QueryM SQL.Flat (RowParser k, Result' a (r, a)))

interpQueries :: ExecQuery m => DepMap -> Result' x a -> StateT Int m QMap
interpQueries dm = Monoid.getAp . (runAp_ $ \case
    NestedQ  {nestedKey = qid, nestedQuery = query } -> Monoid.Ap $ do
       let dat = getDep qid dm
       idx <- get
       (QS {qidGen = idx', updaters=updates},sql, res) <- pure $ runQueryM idx (query dat)
       put idx'
       outv <- execQuery sql
       -- error (SQL.unitSQL sql <> show outv)
       let joins = DepMap $ foldr (M.unionWith (<>)) M.empty [interpJoins vec res | vec <- outv]
       QMap inner <- interpQueries joins res
       let childKey = interpParser (fmap fst res)
       let here = ResultEntry (mkGrouping childKey outv) (fmap snd res) updates
       pure $ QMap (M.insert (theId qid) here inner)
    ParseQ {} ->  mempty)

   
interpReparse :: Result' x a -> x -> a
interpReparse a b =  runIdentity $ runBAp (\x -> \case
    NestedQ {} ->Identity x
    ParseQ {} -> Identity x) a b
interpParser :: Result' x a -> RowParser a
interpParser =  runAp \case
    NestedQ {keyParser=kp, nestedKey=qid} -> do 
        k <- interpParser kp
        (qm, _) <- ask
        pure $ runParserFor qid k qm
    ParseQ {colParser=q} -> q
interpTuple :: Result' x a -> SQL.Tuple
interpTuple = runAp_ \case
    NestedQ {keyParser=p} -> interpTuple p
    ParseQ {cols=t} -> t
interpPrinter :: Result' r a -> r -> UpdatedRows
interpPrinter = runBAp_ $ \x -> \case
    NestedQ {} -> mempty
    ParseQ  {colPrinter=prnt} -> prnt x
interpChildren :: Result' a a -> a -> VMap
interpChildren = runBAp_ $ \x -> \case
    NestedQ {nestedKey=v} -> VMap (M.singleton (theId v) (toDyn x))
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


data ResultEntry = forall r k a. (Ord r, Typeable r, Typeable k, Ord k, Typeable a) => ResultEntry { resultData :: (M.Map k (V.Vector Row)), resultReader :: (Result' a (r, a)), resultWriter :: [Updater] }
unResultEntry1 :: (Typeable k) => ResultEntry -> Maybe (M.Map k (V.Vector Row))
unResultEntry1 (ResultEntry {resultData}) = cast resultData

showResultEntry :: ResultEntry -> String
showResultEntry (ResultEntry {resultData}) = show $ M.elems resultData
showQMap :: QMap -> String
showQMap (QMap m) = unlines [ show k <> showResultEntry v | (k,v) <- M.toList m]

newtype QMap = QMap { unQMap :: M.Map QId ResultEntry }
instance Semigroup QMap where
    QMap a <> QMap b = QMap (M.unionWith (error "key collision") a b)
instance Monoid QMap where
    mempty = QMap M.empty
newtype VMap = VMap { unVMap :: M.Map QId Dynamic }
instance Semigroup VMap where
    VMap a <> VMap b = VMap (M.unionWith (error "VMap collision") a b)
instance Monoid VMap where
    mempty = VMap mempty

data HasMany = HasMany { parentId :: QId, selfId :: QId, parentCol :: String, childCol :: String, table :: String }
  deriving (Eq, Ord, Show, Typeable)

lookupQMapTuple :: QKey k a -> QMap -> SQL.Tuple
lookupQMapTuple qk qmap = do
   case (unQMap qmap) M.! (theId qk) of
     ResultEntry {resultReader} -> interpTuple resultReader
lookupQMapRoot :: QMap -> V.Vector Row
lookupQMapRoot qmap = do
   case (unQMap qmap) M.! QId 0 of
     ResultEntry {resultData} | Just o <- cast resultData -> o M.! ()
     _ -> error "Illegal root parser for qid 0"

lookupQMapParser :: forall k a. (HasCallStack, Typeable a) => QKey k a -> QMap -> RowParser a
lookupQMapParser (QKey qid) qmap = do
   case unQMap qmap M.! qid of
     ResultEntry {resultReader=resultReader}
       | Just o <- cast (fmap snd (interpParser resultReader)) -> o
       | otherwise -> error ("Illegal parer" <> show qid <> ", expected type " <> show (typeOf (undefined :: a)) <> ", got type ")
lookupQMapGrouper :: forall a k r. (HasCallStack, Typeable r) => QKey k a -> QMap -> RowParser r
lookupQMapGrouper (QKey qid) qmap = do
   case (unQMap qmap) M.! qid of
     ResultEntry {resultReader=p}
       | Just o <- cast (fmap fst (interpParser p)) -> o
       | otherwise -> error ("Illegal parer" <> show qid <> ", expected type " <> show (typeOf (undefined :: r)) <> ", got type " )
lookupQMapUnparse :: forall k a. (HasCallStack, Typeable a) => QKey k a -> QMap -> UnParse a
lookupQMapUnparse (QKey qid) qmap = do
   case (unQMap qmap) M.! qid of
     ResultEntry {resultReader=p}
       | Just o <- cast (interpReparse p) -> o
       | otherwise -> error ("Illegal parer" <> show qid <> ", expected type " <> show (typeOf (undefined :: a)) <> ", got type ")


deltaRows :: forall k a. (Typeable k, Typeable a) => k -> QKey k a -> QMap -> [a] -> [(Maybe Row, Maybe Row')]
deltaRows k (QKey qid) qmap as = 
   case (unQMap qmap) M.! qid of
     ResultEntry @_ @k' @a' v res _ -> case (eqT @k @k', eqT @a @a') of
       (Just Refl, Just Refl) -> let
            (rowParser, groupVal, unParse) = (interpParser (fmap fst res), interpReparse (fmap fst res), interpPrinter res)
            oldRows = M.fromList [(runRowParser rowParser qmap e, e) | e <- V.toList (v M.! k) ]
            newRows = M.fromList [ (groupVal a, row) | a <- as, let row = unParse a ]
            merged = M.mergeWithKey (\_ old new -> Just (Just old, Just new)) (M.map (\x -> (Just x, Nothing))) (M.map (\x -> (Nothing, Just x))) oldRows newRows
        in undefined -- M.elems merged
       _ -> error ("Illegal parer" <> show qid <> ", expected type " <> show (typeOf (undefined :: a)) <> ", got type " <> show (typeOf @a' undefined))

qmapType :: ResultEntry -> String
qmapType (ResultEntry @_ @k _ _ _) = show (typeOf @k undefined)

lookupQMap :: forall k. (Typeable k, Ord k) => QId -> k -> QMap -> Maybe (V.Vector Row)
lookupQMap qid k qmap = do
   mrm <- M.lookup qid (unQMap qmap)
   rm <- unResultEntry1 mrm
   pure $ M.findWithDefault mempty k rm
data QKey (k::Type) (a::Type) = QKey {theId :: QId}
data ParserState = ParserState {
    curData :: QMap,
    curRow :: Row
}

loadNested :: (Monad m, ExecQuery m, Ord r, Typeable r, Typeable a) => QueryM SQL.Flat (Result' a (k, (r, a))) -> m QMap
loadNested query = do
    (QS {qidGen = idx', updaters=upds},sql,res) <- pure $ runQueryM 1 query
    rows <- execQuery sql
    let v = V.fromList rows
    let deps = mconcat [DepMap (interpJoins row res) | row <- V.toList v]
    QMap out <- evalStateT (interpQueries deps res) idx'
    pure $ QMap $ M.insert (QId 0 ) (ResultEntry (M.singleton () v) (fmap snd res) upds) out

runParserRoot :: (HasCallStack, Typeable a) => QMap -> [a]
runParserRoot qmap = runParserFor (QKey (QId 0)) () qmap

runParserFor :: forall k a. (HasCallStack, Typeable a, Typeable k, Ord k) => QKey k a -> k -> QMap -> [a]
runParserFor qkey k qmap = map (runRowParser pars qmap) (V.toList root)
  where
    -- !_ = if typeOf @a undefined == typeOf @(Account, [Customer]) undefined then () else error (show (typeOf @a undefined) <> showQMap qmap)
    pars = lookupQMapParser qkey qmap
    root = case (lookupQMap (theId qkey) k qmap) of
        Nothing -> error ("No results for " <> show (theId qkey) <> " and key " <> show (typeOf @k undefined))
        Just x -> x

