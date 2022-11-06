{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE NoFieldSelectors    #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}

{-# OPTIONS_GHC -Wno-deprecations #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use record patterns" #-}
module Classes where
import GHC.Stack (HasCallStack)
import Database.HDBC.Sqlite3 (connectSqlite3)
import qualified Data.Vector as V
import Data.String (IsString (fromString))
import qualified Data.Map as M
import Data.Typeable
import Control.Monad.State.Strict
import Control.Monad.Reader

import Database.Relational.OverloadedInstances ()
import Entity.Customer ( Customer(custId), customer )
import Entity.Account ( Account(accountId), account, availBalance )
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
import qualified Language.SQL.Keyword.Type as Words
import Database.Relational.Internal.UntypedTable as UT
import qualified Data.Map.Merge.Strict as M
import qualified Database.Record.KeyConstraint as CK
import qualified Database.Relational.Table as Table
import qualified Language.SQL.Keyword.Concat as Words
import Data.Maybe (fromMaybe)
import Data.Foldable (traverse_)
data UpdateStep = UpsertNow (IO [(SQL.StringSQL, SQLV.SqlValue)]) | DeleteLater (IO ())

data Step a = Insert a RawRow | Delete RawRow | Update RawRow a RawRow
  deriving Show

-- recursive \rec -> do
--    u <- query users
--    wheres (u.id .=. value 1)
--    children <- rec u.children
--    pure (select u & #children .= children)

-- recursive :: QueryM (Record a) (RelationN a a) -> (Record a -> Result [r] -> QueryM (Result r)) -> QueryM (Result r)
-- recursive2 :: QueryM (Record a) (Relation a a) -> (Record a -> Result r -> QueryM (Result r)) -> QueryM (Result r)
 -- (\rec r -> do
 --   o <- r.children
 --   out <- rec o)


-- recursive (query children) #children (\c -> select u & #children .= c)
-- WITH RECURSIVE u AS (
--   (SELECT * FROM users WHERE id = 1)
--   UNION DISTINCT
--   (SELECT * FROM users WHERE parent_id = u.id)
-- )
-- SELECT * FROM u
    

(!!!) :: (HasCallStack, Ord a) => M.Map a b -> a -> b
m !!! k = case M.lookup k m of
  Nothing -> error "Key not found: "
  Just v -> v

execUpdate :: Updater -> Step a -> (Words.Keyword, [SQLV.SqlValue])
-- INSERT INTO $table (*columns,) VALUES (*?,) {$args}
execUpdate _ (Insert _ r) = (mconcat sql <> cols <> values, M.elems r.values)
  where
    sql = [Words.INSERT, Words.INTO, Words.word r.tableName]
    cols = tuple $ map fromString (M.keys (r.values))
    values = Words.VALUES <> tuple (replicate (M.size r.values) "?")
    parens a = "(" Words.<++> a Words.<++> ")"
    tuple = parens . sepByComma
    -- returns = Words.RETURNING <> tuple (map fromString $ M.keys inWhere.values)

    sepByComma = foldl1 (Words.|*|)
-- UPDATE $table SET *$key=?, WHERE *$pkey=? {args <> primary_key_args}
execUpdate updater (Update l _ r) 
  | M.intersection l.values  r.values /= r.values = (mconcat sql <> values <> whereClause, M.elems inSet.values <> M.elems inWhere.values)
  where
    sql = [Words.UPDATE, Words.word r.tableName, Words.SET]
    -- cols = sepByComma [k <> "= ?" | k <- M.keys 
    values = Words.VALUES <> sepByComma [fromString k <> "= ?" | k <- M.keys (inSet.values)]

    whereClause = Words.WHERE <> tuple [fromString k <> "= ?" | k <- M.keys (inWhere.values)]
    parens a = "(" Words.<++> a Words.<++> ")"
    tuple = parens . sepByComma

    (_, inSet) = splitRow updater l
    (inWhere, _) = splitRow updater r

    sepByComma = foldl1 (Words.|*|)
execUpdate _ _ = (mempty, mempty)

critColumns :: MetaData -> S.Set String
critColumns md = S.fromList [Words.wordShow col | (idx, col) <- zip [0..] cols, idx `elem` primIdxs]
  where
    cols = UT.columns' md.table
    primIdxs = S.fromList md.primaryKeys
splitRow :: Updater -> RawRow -> (RawRow, RawRow)
splitRow u rawRow = (RawRow rawRow.tableName left, RawRow rawRow.tableName right)
  where
    (left, right) = M.partitionWithKey (\k _ -> k `S.member` cols) rawRow.values
    cols = critColumns (fromMaybe (error (show (u.metaDatas, rawRow.tableName))) (M.lookup rawRow.tableName u.metaDatas ))
data RawRow = RawRow { tableName :: String, values :: M.Map String SQLV.SqlValue }
  deriving Show
newtype UpdatedRows' f = UR (M.Map Int f)
  deriving Show
type UpdatedRows = UpdatedRows' (Maybe RawRow)
type RawRows = UpdatedRows' RawRow
instance Semigroup UpdatedRows where
  (<>) :: UpdatedRows -> UpdatedRows -> UpdatedRows
  UR a <> UR b = UR (M.unionWith (<>) a b)
instance Monoid UpdatedRows where
    mempty :: UpdatedRows
    mempty = UR M.empty
data UpdatedRow = DeleteRow | SetRow (M.Map Col SQLV.SqlValue)
    deriving (Show)
instance Semigroup UpdatedRow where
    (<>) :: UpdatedRow -> UpdatedRow -> UpdatedRow
    (<>) DeleteRow DeleteRow = DeleteRow
    (<>) (SetRow a) (SetRow b) = SetRow (a <> b)
    (<>) a b = error $ "UpdatedRow: " <> show a <> " <> " <> show b
type QueryRef = SQL.Qualified Int
data Updater = Updater {
    metaDatas :: M.Map String MetaData,
    propagation :: [(Col, Col)]
} deriving Show
instance Semigroup Updater where
   (Updater a b) <> (Updater c d) = Updater (a <> c) (b <> d)
instance Monoid Updater where
    mempty = Updater mempty mempty

thePrimaryKey :: forall a. CK.HasColumnConstraint CK.Primary a => [Int]
thePrimaryKey = CK.indexes $  CK.derivedCompositePrimary @a

data MetaData = MD { table :: UT.UTable, primaryKeys :: [Int] }
 deriving Show
tableData :: forall a. (CK.HasColumnConstraint CK.Primary a, SQL.TableDerivable a) => MetaData
tableData = MD ut pk
  where
    ut = Table.untype (SQL.derivedTable @a)
    pk = thePrimaryKey @a

data Col = Col { fromIdx :: Int, colTable :: String, colName :: String }
    deriving (Show, Eq, Ord)
mkCol :: M.Map Int UT.UTable -> Int -> String -> Col
mkCol ut idx = Col idx (UT.name' (ut !!! idx))

tableMappings :: SQL.SubQuery -> M.Map Int UT.UTable
tableMappings sq0= case sq0 of
     (SQL.Flat _ _ _ (Just pt) _ _) ->  go pt
     (SQL.Aggregated _ _ _ (Just pt) _ _ _ _) ->  go pt
     (SQL.Bin _ a b) ->  tableMappings a <> tableMappings b
     _ -> mempty
  where
    go (SQL.Leaf (_, SQL.Qualified (SQL.Qualifier i)  (SQL.Table a))) = M.singleton i a  
    go (SQL.Join (SQL.Node _ a) (SQL.Node _ b) _) = go a <> go b
    go _ = mempty
splitString :: HasCallStack => SQL.StringSQL -> (Int, String)
splitString seque = 
   case  break (=='.') (Words.wordShow seque) of
    ('T':l,'.':r)  -> (read l,r)
    _ -> error ("Invalid split input: " <> show seque)
    
  
diffRow :: forall a. Maybe RawRows -> Maybe (a, UpdatedRows) -> M.Map Int (Step a)
diffRow (Just (UR r)) Nothing = M.map Delete r
diffRow Nothing Nothing = error "foo"
diffRow Nothing (Just (a, UR m)) = M.mapMaybe (\case
    Just x -> Just (Insert a x)
    _ -> Nothing) m
diffRow (Just (UR m)) (Just (a, UR ms)) = out
  where
    out :: M.Map Int (Step a)
    out = M.merge M.dropMissing (M.mapMaybeMissing (\_ -> \case
      (Just x) -> Just (Insert a x)
      _ -> Nothing)) (M.zipWithMaybeMatched inner) m ms
    inner :: Int -> RawRow -> Maybe RawRow -> Maybe (Step a)
    inner _ l@(RawRow t x) (Just r@(RawRow t2 b)) 
      | t /= t2  = error ("trying to diff different table refs: " <> show (t,t2))
      | otherwise = Just (Update l a r)
    inner _ _ _  = Nothing



testQ :: QueryM SQL.Flat (AResult Int (Account, [Customer]))
testQ = do
    accQ <- SQL.query account
    tellMetadata @Account
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

instance (x ~ JoinConfig Int Customer Singular) => ORMField "customer" Account x where
    ormField r = JoinConfig {joinKeyR = #custId, joinTarget = customer, joinFinalizer = singular, joinOrigin = r.custId  }

instance (CustomerField l b, FundepHack l Customer b, ORMField l Customer b) => HasField l (Record.Record SQL.Flat Customer) b where
    getField = ormField @l
instance (AccountField l b, FundepHack l Account b, ORMField l Account b) => HasField l (Record.Record SQL.Flat Account) b where
    getField = ormField @l
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
   (((((x, res), ot), rs), pd), da) <- extract q
   c <- SQL.askConfig
   let tups = interpTuple res
   pure (x, SQL.flatSubQuery c tups da pd (map Record.untype rs) ot, res)
  where
    extract =  SQL.extractCore . SQL.extractOrderingTerms

data QueryState = QS { qidGen :: Int, selects :: [SQL.Column], updaters :: Updater }
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

mkJoin :: forall k b r f fd . (
    SQLV.FromSql HDBC.SqlValue k,
    SQLP.PersistableWidth k,
    SQLP.PersistableWidth b,
    SQL.LiteralSQL k,
    Show k,
    Typeable k,
    Typeable r,
    Typeable fd,
    Ord fd,
    Ord k, Record.ToSql HDBC.SqlValue k, CK.HasColumnConstraint CK.Primary b, Table.TableDerivable b
  ) => JoinConfig k b f -> (Record.Record SQL.Flat b -> QueryM SQL.Flat (Result' r (fd, r))) ->  QueryM SQL.Flat (Result' [r] [r])
mkJoin cfg parse = do

      nested (sel cfg.joinOrigin ) $ \row -> do
        tellMetadata @b
        child <- SQL.query cfg.joinTarget 
        let key = child SQL.!  cfg.joinKeyR 
        SQL.wheres $ SQL.in' key (SQL.values row)
        res <- parse child
        pure $ liftA2 (,) (undefined =. sel key) res

type UnParse r = r -> [(SQL.Column, SQLV.SqlValue)]

nested :: (Show k, Typeable a, Typeable k, Ord k, Typeable r, Ord r) => Result k -> ([k] -> QueryM SQL.Flat (Result' a (k, (r, a)))) -> QueryM m (Result' [a] [a])
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
   put qs { qidGen = qs.qidGen  + 1 }
   pure $ QId qs.qidGen
tellMetadata :: forall a m. (CK.HasColumnConstraint CK.Primary a, SQL.TableDerivable a) => QueryM m ()
tellMetadata = modify $ \s -> s { updaters = Updater (M.singleton (UT.name' td.table) td) [] <> s.updaters }
  where td = tableData @a
sel :: forall a c. (HasCallStack, SQLV.FromSql SQLV.SqlValue a, SQLP.PersistableWidth a, Record.ToSql HDBC.SqlValue a) => SQL.Record c a -> Result' a a
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
    decRows m a = decideUpdatesDefault (Record.untype rec) m (Record.runFromRecord Record.recordToSql a :: [SQLV.SqlValue])

resolveColumn :: M.Map Int UTable -> SQL.Column -> Maybe Col
resolveColumn utab (SQL.RawColumn s) = Just $ Col a (UT.name' $ utab !!! a) b
  where (a,b) = splitString s
resolveColumn _ _ = Nothing
decideUpdatesDefault :: SQL.Tuple -> M.Map Int UTable -> [HDBC.SqlValue] -> UpdatedRows
decideUpdatesDefault tups m vals = UR $ M.fromListWith (<>) $ do
    (SQL.RawColumn str, val) <- zip tups vals
    let (i,col) = splitString str
        table = UT.name' (m !!! i)
    pure (i, Just $ RawRow table (M.singleton col val))

newtype QueryM f a = QueryM { unQueryM :: StateT QueryState QueryT a }
  deriving (Functor, Applicative, Monad, MonadState QueryState)
instance MonadRestrict SQL.Flat (QueryM SQL.Flat) where
   restrict p = QueryM (lift $ SQL.restrict p)
instance  MonadQualify SQL.ConfigureQuery (QueryM f) where
   liftQualify p = QueryM (lift $ liftQualify p)
instance  MonadQuery (QueryM f) where
   setDuplication :: SQL.Duplication -> QueryM f ()
   setDuplication = QueryM . lift . setDuplication
   restrictJoin :: Record.Predicate SQL.Flat -> QueryM f ()
   restrictJoin = QueryM . lift . restrictJoin
   query' :: SQL.Relation p r -> QueryM f (SQL.PlaceHolders p, Record.Record SQL.Flat r)
   query' = QueryM . lift . query'
   queryMaybe' :: SQL.Relation p r -> QueryM f (SQL.PlaceHolders p, Record.Record SQL.Flat (Maybe r))
   queryMaybe' = QueryM . lift . queryMaybe'

type M a = StateT QueryState QueryT a
runQueryM :: forall n a. Int -> QueryM SQL.Flat (Result' n a) -> (QueryState, SQL.SubQuery, Result' n a)
runQueryM id0 (QueryM q) = out 
  where
    ms = runStateT q (QS id0 mempty mempty)
    ns = do
      (res,queryState) <- ms
      pure (queryState,res)
    out = SQL.configureQuery (toSubQuery ns) SQL.defaultConfig

runInsert :: (Ord r, Typeable r, Typeable a) => QueryM SQL.Flat (Result' a (k, (r, a))) -> [a] -> IO ()
runInsert query a = do
    (QS {qidGen = idx', updaters=upds},sql,res) <- pure $ runQueryM 1 query
    let QMap qmap = evalState (interpWOQueries res) 0
        out = QMap $ M.insert (QId 0 ) (ResultEntry (M.empty @()) (fmap snd res) upds (tableMappings sql)) qmap
        diffs = concatMap M.elems $ map (uncurry diffRow) (toDeltaRoot a out) 
    -- traverse (execUpdate ) out
    undefined -- pure out 

runUpdate :: forall a k. (Typeable a, Typeable k) => k -> QKey k a -> QMap -> [a] -> IO ()
runUpdate k qk qm as = do
    let 
        deltas = deltaRows k qk qm as
        diffs = map M.elems $ map (uncurry diffRow) deltas
        sqls :: [Step a] -> IO ()
        sqls diff = do
          let sql = map (execUpdate (lookupQMapUpdater qk qm)) diff
          undefined
        inner parentRow a = processChildren parentRow a qk qm
    undefined
    
processChildren :: Typeable a => RawRows -> a -> QKey k a -> QMap -> IO ()
processChildren row a qk qm = traverse_ (uncurry step) (M.toList children)
  where
    result = lookupQMapResult qk qm
    VMap children = interpChildren result a

    deps = interpRefs row result
    step qid dynamic = case qm.unQMap !!! qid of
      ResultEntry {resultReader = _ :: Result' b (r,b), resultData = _ :: M.Map k' (V.Vector Row)}  
        | Just as' <- fromDynamic @[b] dynamic
        , Just k <- fromDynamic @k' (deps M.! qid) -> do
        runUpdate k (QKey qid) qm as'
      _ -> error "Illegal step"

runTestQ :: IO [(Account, [Customer])]
runTestQ = do
    conn <- connectSqlite3 "examples.db"
    (qmap,a) <- runReaderT (runAQuery testQ) conn
    let out = concatMap M.elems (uncurry diffRow <$> toDeltaRoot (fmap (\(x,y) -> (x{availBalance=fmap(+1) x.availBalance},y)) a) qmap)
        upd = lookupQMapUpdater (QKey $ QId 0) qmap 
    mapM_ print $ execUpdate upd <$> out
    pure []
    -- pure a

singular :: [a] -> a
singular [a] = a
singular _ = error "Invalid query"

asRoot :: QueryM SQL.Flat (Result' r a) -> QueryM SQL.Flat (Result' r ((), a))
asRoot = fmap (fmap ((),))

runAQuery :: (Typeable a, ExecQuery m, Typeable r, Ord r) => QueryM SQL.Flat (Result' a (r, a)) -> m (QMap, [a])
runAQuery q = do
   qmap <- loadNested (asRoot q)
   pure (qmap, runParserRoot qmap)

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
runKeyParser rp = runRowParser rp undefined


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
    ParseQ :: { cols :: SQL.Tuple, colPrinter ::  M.Map Int UTable -> a -> UpdatedRows , colParser :: RowParser a } -> ResultF a a
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
interpJoins r res = (runKeyParser inner r).unDepMap
  where
    inner = Monoid.getAp (runAp_ (Monoid.Ap . step) res)

    step :: ResultF x a -> RowParser DepMap
    step NestedQ {keyParser = p, nestedKey = qid} = (\o -> DepMap (M.singleton qid.theId (DepSet $ S.singleton o))) <$> interpParser p
    step ParseQ {colParser=keyParser} =  mempty <$ keyParser

interpRefs :: RawRows -> Result' x a -> M.Map QId Dynamic
interpRefs r res = inner
  where
    inner = runAp_  step res

    step :: ResultF x a -> M.Map QId Dynamic
    step NestedQ {keyParser = p, nestedKey = qid} = M.singleton qid.theId $ toDyn $ interpRawParser p r
    step ParseQ {} =  mempty


data SomeQuery = forall k a r.  (Typeable k, Typeable r, Typeable a) => SomeQuery ([k] -> QueryM SQL.Flat (RowParser k, Result' a (r, a)))

interpQueries :: ExecQuery m => DepMap -> Result' x a -> StateT Int m QMap
interpQueries dm = Monoid.getAp . runAp_ (\case
    NestedQ  {nestedKey = qid, nestedQuery = query } -> Monoid.Ap $ do
       let dat = getDep qid dm
       idx <- get
       (QS {qidGen = idx', updaters=updates},sql, res) <- pure $ runQueryM idx (query dat)
       put idx'
       outv <- execQuery sql
       let joins = DepMap $ foldr (M.unionWith (<>)) M.empty [interpJoins vec res | vec <- outv]
       QMap inner <- interpQueries joins res
       let childKey = interpParser (fmap fst res)
       let here = ResultEntry (mkGrouping childKey outv) (fmap snd res) updates (tableMappings sql)
       pure $ QMap (M.insert qid.theId here inner)
    ParseQ {} ->  mempty)
interpWOQueries :: Monad m => Result' x a -> StateT Int m QMap
interpWOQueries = Monoid.getAp . runAp_ (\case
    NestedQ  {nestedKey = qid, nestedQuery = query } -> Monoid.Ap $ do
       let dat = mempty
       idx <- get
       (QS {qidGen = idx', updaters=updates},sql, res) <- pure $ runQueryM idx (query dat)
       put idx'
       QMap inner <- interpWOQueries res
       let here = ResultEntry (mempty :: M.Map () (V.Vector Row)) (fmap snd res) updates (tableMappings sql)
       pure $ QMap (M.insert qid.theId here inner)
    ParseQ {} ->  mempty)

interpRawParser :: Result' x a -> RawRows -> a
interpRawParser a m =  runIdentity $ runAp (\case
    NestedQ {} -> Identity []
    ParseQ {cols, colParser} -> 
      let vec = V.fromList $ map (lookupRawRow m) cols
      in Identity (runKeyParser colParser vec)) a

lookupRawRow :: RawRows -> SQL.Column -> HDBC.SqlValue
lookupRawRow (UR raw) (SQL.RawColumn s) = (raw M.! a).values M.! b
  where (a,b) = splitString s
lookupRawRow _ _ = error "boo"

    
   
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
interpPrinter :: M.Map Int UT.UTable -> Result' r a -> r -> UpdatedRows
interpPrinter m = runBAp_ $ \x -> \case
    NestedQ {} -> mempty
    ParseQ  {colPrinter=prnt} -> prnt m x
interpChildren :: Result' a a -> a -> VMap
interpChildren = runBAp_ $ \x -> \case
    NestedQ {nestedKey=v} -> VMap (M.singleton v.theId  (toDyn x))
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


data ResultEntry = forall r k a. (Ord r, Typeable r, Typeable k, Ord k, Typeable a) => ResultEntry { resultData :: M.Map k (V.Vector Row), resultReader :: Result' a (r, a), resultWriter :: Updater, resultMappings :: M.Map Int UT.UTable }
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
   case qmap.unQMap !!! qk.theId of
     ResultEntry {resultReader} -> interpTuple resultReader

lookupQMapUpdater :: QKey k a -> QMap -> Updater
lookupQMapUpdater qk qmap = do
   case qmap.unQMap !!! qk.theId of
     ResultEntry {resultWriter} -> resultWriter

lookupQMapRoot :: QMap -> V.Vector Row
lookupQMapRoot qmap = do
   case qmap.unQMap !!! QId 0 of
     ResultEntry {resultData} | Just o <- cast resultData -> o !!! ()
     _ -> error "Illegal root parser for qid 0"

lookupQMapParser :: forall k a. (HasCallStack, Typeable a) => QKey k a -> QMap -> RowParser a
lookupQMapParser (QKey qid) qmap = do
   case qmap.unQMap !!! qid of
     ResultEntry {resultReader=resultReader}
       | Just o <- cast (fmap snd (interpParser resultReader)) -> o
       | otherwise -> error ("Illegal parer" <> show qid <> ", expected type " <> show (typeRep @_ @a undefined) <> ", got type ")

lookupQMapResult :: forall k a. (HasCallStack, Typeable a) => QKey k a -> QMap -> Result a
lookupQMapResult (QKey qid) qmap = do
   case qmap.unQMap !!! qid of
     ResultEntry {resultReader=resultReader}
       | Just o <- cast (fmap snd resultReader) -> o
       | otherwise -> error ("Illegal parer" <> show qid <> ", expected type " <> show (typeRep @_ @a undefined) <> ", got type ")
lookupQMapGrouper :: forall a k r. (HasCallStack, Typeable r) => QKey k a -> QMap -> RowParser r
lookupQMapGrouper (QKey qid) qmap = do
   case qmap.unQMap !!! qid of
     ResultEntry {resultReader=p}
       | Just o <- cast (fmap fst (interpParser p)) -> o
       | otherwise -> error ("Illegal parer" <> show qid <> ", expected type " <> show (typeRep @_ @r undefined) <> ", got type " )
lookupQMapUnparse :: forall k a. (HasCallStack, Typeable a) => QKey k a -> QMap -> UnParse a
lookupQMapUnparse (QKey qid) qmap = do
   case qmap.unQMap !!! qid of
     ResultEntry {resultReader=p}
       | Just o <- cast (interpReparse p) -> o
       | otherwise -> error ("Illegal parer" <> show qid <> ", expected type " <> show (typeRep @_ @a undefined) <> ", got type ")

toDeltaRoot :: Typeable a => [a] -> QMap -> [(Maybe RawRows, Maybe (a, UpdatedRows))]
toDeltaRoot ls q = deltaRows () (QKey (QId 0)) q ls

deltaRows :: forall k a. (Typeable k, Typeable a) => k -> QKey k a -> QMap -> [a] -> [(Maybe RawRows, Maybe (a, UpdatedRows))]
deltaRows k (QKey qid) qmap as = 
   case qmap.unQMap !!! qid of
     ResultEntry @_ @k' @a' v res _ utab -> case (eqT @k @k', eqT @a @a') of
       (Just Refl, Just Refl) -> let
            (rowParser, groupVal, unParse, tuples) = (interpParser (fmap fst res), interpReparse (fmap fst res), interpPrinter utab res, interpTuple res)
            oldRows = M.fromList [(runRowParser rowParser qmap e, labelRow tuples utab e) | e <- V.toList (v !!! k) ]
            newRows = M.fromList [ (groupVal a, (a, row)) | a <- as, let row = unParse a ]
            merged = M.mergeWithKey (\_ old new -> Just (Just old, Just new)) (M.map (\x -> (Just x, Nothing))) (M.map (\x -> (Nothing, Just x))) oldRows newRows
        in M.elems merged
       _ -> error ("Illegal parer" <> show qid <> ", expected type " <> show (typeRep @_ @a undefined) <> ", got type " <> show (typeOf @a' undefined))

instance Semigroup RawRow where
  RawRow t1 v1 <> RawRow t2 v2 
    | t1 == t2 = RawRow t1 (M.unionWithKey (\k a b -> if a == b then a else error $ "Incompatible values set for same column, no single source of truth: " <> show (t1, k, a,b)) v1 v2)
    | otherwise = error "Illegal rawRow merge; the select source should uniquely determine the source table"
labelRow :: SQL.Tuple -> M.Map Int UTable -> Row -> RawRows
labelRow tuple umap row = UR $ M.fromListWith (<>) [ (qIdx, RawRow table (M.singleton col r)) | (t,r) <- zip tuple (V.toList row), Just (Col qIdx table col) <- [resolveColumn umap t] ]


qmapType :: ResultEntry -> String
qmapType (ResultEntry @_ @k _ _ _ _) = show (typeOf @k undefined)

lookupQMap :: forall k a. (Typeable k, Ord k) => QKey k a -> k -> QMap -> Maybe (V.Vector Row)
lookupQMap qid k qmap = do
   mrm <- M.lookup qid.theId qmap.unQMap
   rm <- unResultEntry1 mrm
   pure $ M.findWithDefault mempty k rm
newtype QKey (k::Type) (a::Type) = QKey {theId :: QId}
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
    pure $ QMap $ M.insert (QId 0 ) (ResultEntry (M.singleton () v) (fmap snd res) upds (tableMappings sql)) out

runParserRoot :: (HasCallStack, Typeable a) => QMap -> [a]
runParserRoot = runParserFor (QKey (QId 0)) ()

runParserFor :: forall k a. (HasCallStack, Typeable a, Typeable k, Ord k) => QKey k a -> k -> QMap -> [a]
runParserFor qkey k qmap = map (runRowParser pars qmap) (V.toList root)
  where
    -- !_ = if typeOf @a undefined == typeOf @(Account, [Customer]) undefined then () else error (show (typeOf @a undefined) <> showQMap qmap)
    pars = lookupQMapParser qkey qmap
    root = case lookupQMap qkey k qmap of
        Nothing -> error ("No results for " <> show qkey.theId <> " and key " <> show (typeOf @k undefined))
        Just x -> x

