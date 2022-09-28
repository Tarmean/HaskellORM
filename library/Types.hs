{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE DefaultSignatures #-}
module Types where
import Control.Monad.State as S
import Data.List (intercalate)
-- import qualified Database.MySQL.Base as Q
-- import qualified System.IO.Streams as Streams
import Data.Foldable (toList)
import Data.Typeable

import qualified Data.Map as M
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Char8 as BS
import Control.Applicative
import Control.Monad.Writer
import Control.Monad.Trans.Maybe
import qualified Data.Text as T
import Data.Coerce (coerce)
import GHC.Stack (HasCallStack)
import Data.Maybe (fromMaybe)
import Data.Functor.Compose (Compose (Compose), getCompose)
import Data.Containers.ListUtils (nubOrd)
import Control.Monad.Reader


data QueryBuilder = QB { queryParts :: BasicQuery, queryParams :: BasicQueryParams, queryNested :: [NestedCall], querySelected :: [SomeSelect] }
instance Semigroup QueryBuilder where
    QB a b c d <> QB a' b' c' d' = QB (a <> a') (b <> b') (c <> c') (d <> d')
instance Monoid QueryBuilder where
    mempty = QB mempty mempty mempty mempty
instance Semigroup (BasicQuery' s) where
    (BasicQuery s1 w1 o1 f1 l1) <> (BasicQuery s2 w2 o2 f2 l2) = BasicQuery (s1 <> s2) (w1 <> w2) (o1 <> o2) (f1 <> f2) (l1 <> l2)
instance Monoid (BasicQuery' s) where
    mempty = BasicQuery mempty mempty mempty mempty mempty
data Delta a = Insert a | Delete a | Update a a | Noop a
    deriving (Eq, Ord, Show, Functor)
data BasicQuery' s = BasicQuery { select :: [s], qFrom :: [s],qWhere :: [s], qOrder :: [s],  qLimit :: [s] }
  deriving (Eq, Show, Foldable)
type BasicQuery = BasicQuery' String
type BasicQueryParams = BasicQuery' ()

-- type Value = Q.MySQLValue
-- newtype Column = Column { unColumn :: BS.ByteString}
--     deriving (Eq, Ord, Show)
newtype SqlOut = SqlOut {unSqlOut :: [Row]}
type Row = ()
-- newtype SqlOut = SqlOut { offsets :: M.Map SomeSelect Q.MySQLValue }
--   deriving (Eq, Show)
-- instance Semigroup SqlOut where
--   (<>) a b = SqlOut $ M.unionWith (\l r -> if l == r then l else error "duplicate key") (offsets a) (offsets b)
-- instance Monoid SqlOut where
--     mempty = SqlOut M.empty
data NestedCall where
  NestedCall :: Typeable a => ([SqlOut] -> IO (SqlOut -> a)) -> NestedCall
-- data NestedResult where
--   NestedResult :: Typeable a => (SqlOut -> a) -> NestedResult
-- runNestedResult :: Typeable a => SqlOut -> NestedResult -> a
-- runNestedResult out (NestedResult f) = case cast (f out) of
--   Nothing -> error ("runNestedResult: type mismatch" <> show (typeOf (f out)))
--   Just a -> a


-- mkFrom :: String -> [Q.Param] -> QueryBuilder
-- mkFrom s p = mempty { queryParts = mempty { qFrom = [s] }, queryParams = mempty { qFrom = p } }

-- mkWhere :: String -> [Q.Param] -> QueryBuilder
-- mkWhere s p = mempty { queryParts = mempty { qWhere = [s] }, queryParams = mempty { qWhere = p } }
-- mkLimit :: String -> [Q.Param] -> QueryBuilder
-- mkLimit s p = mempty { queryParts = mempty { qLimit = [s] }, queryParams = mempty { qLimit = p } }
-- mkSelect :: ARef s -> ColOf s a -> QueryBuilder
-- mkSelect s p = mempty { queryParts = mempty { select = [] }, querySelected = [SomeSelect s p] }

-- dot :: ARef a -> ColOf a c -> String
-- dot (ARef a) (ColOf c _) = "`" <> a <> "`.`" <> c <> "`"
-- whereIn :: ARef a -> ColOf a c -> [Q.MySQLValue] -> QueryBuilder
-- whereIn ref column ins = mkWhere (dot ref column <> " IN (?)") [Q.Many $ toList ins]


-- formatQuery :: QueryBuilder -> Q.Query
-- formatQuery QB{ queryParts=query, querySelected } = Q.Query $ B.toLazyByteString $ B.byteString "SELECT " <> B.byteString tSelect <> tFrom <> tWhere <> tOrder <> tLimit
--   where
--     directSelect = map (\(SomeSelect s t) -> dot s t) $ nubOrd querySelected
--     tSelect = BS.pack $ intercalate ", " (select query <> directSelect)
--     tFrom
--       | null $ qFrom query = ""
--       | otherwise = B.byteString " FROM " <> B.byteString (BS.pack $ intercalate ", " $ qFrom query)
--     tWhere
--       | null $ qWhere query = ""
--       | otherwise = B.byteString " WHERE " <> B.byteString (BS.pack $ intercalate " AND " $ qWhere query)
--     tOrder
--         | null $ qOrder query = ""
--         | otherwise = B.byteString " ORDER BY " <> B.byteString (BS.pack $ intercalate ", " $ qOrder query)
--     tLimit
--         | null $ qLimit query = ""
--         | otherwise = B.byteString " LIMIT " <> B.byteString (BS.pack $ intercalate ", " $ qLimit query)


-- testConnection :: IO Q.MySQLConn
-- testConnection = Q.connect Q.defaultConnectInfo { Q.ciHost = "localhost", Q.ciUser = "root", Q.ciPassword = "", Q.ciDatabase = "b'www-3'", Q.ciPort =3306 }
-- newtype ATable a = ATable { unTable :: String}
--   deriving (Eq, Ord, Show)
newtype ARef a = ARef { unARef :: String}
  deriving (Eq, Ord, Show)
data IsoSql b = IsoSql { fromSql :: b -> b, toSql :: b -> b }
data ColOf a b = ColOf { colName :: String, colIso :: IsoSql b }
-- class (forall x. Applicative (m x)) => ORM m where
--     limit :: Int -> m r ()
--     col :: ARef a -> ColOf a b -> m b b
--     fromWith :: ATable a -> ARef a -> m r ()
--     nestedWith :: (Typeable o, LoadRelation r s) => r -> ARef s -> (forall n. ORM n => n o o) -> m [o] [o]
--     (=.) :: (s -> r) -> m r a -> m s a
--     readonly :: m a b -> m x ()
-- ensureParentDependencyLoaded :: (ORM m, LoadRelation r s) => r -> m x ()
-- ensureParentDependencyLoaded rs = go (parentCol rs)
--   where
--     go (SomeSelect a c:xs) = readonly (col a c) *> go xs
--     go [] = pure ()
-- ensureChildDependencyLoaded :: (ORM m, LoadRelation r s) => r -> ARef s -> m x ()
-- ensureChildDependencyLoaded rs ref = go (childCol rs ref)
--   where
--     go (SomeSelect a c:xs) = readonly (col a c) *> go xs
--     go [] = pure ()

-- data User
-- users :: ATable User
-- users = ATable "users"
-- userFirstName :: ColOf User String
-- userFirstName = ColOf "firstname" sqlString
-- userId :: ColOf User Int
-- userId = ColOf "id" sqlInt32U
-- sqlString :: IsoSql String
-- sqlString = IsoSql fromT (Q.MySQLText . T.pack)
--   where
--     fromT (Q.MySQLText t) = T.unpack t
--     fromT o = error $ "Expected MySQLText, got " ++ show o
-- sqlInt64 :: IsoSql Int
-- sqlInt64 = IsoSql fromT (Q.MySQLInt64 . fromIntegral)
--   where
--     fromT (Q.MySQLInt64 i) = fromIntegral i
--     fromT o = error $ "Expected MySQLInt64, got " ++ show o
-- sqlInt32U :: IsoSql Int
-- sqlInt32U = IsoSql fromT (Q.MySQLInt32U . fromIntegral)
--   where
--     fromT (Q.MySQLInt32U i) = fromIntegral i
--     fromT o = error $ "Expected MySQLInt64, got " ++ show o

-- type CollectQuery = NameGen QueryA
-- newtype QueryA r a = QueryA { runQueryA :: MaybeT (StateT (M.Map Char Int) (Writer QueryBuilder)) a }
--     deriving (Alternative)
-- tellQuery :: QueryBuilder -> QueryA r ()
-- tellQuery q = QueryA $ tell q
-- instance Applicative (QueryA r) where
--     pure a = QueryA $ pure a
--     l <*> r = QueryA $ MaybeT $ StateT $ \s -> writer $
--       case runWriter (runStateT (runMaybeT (runQueryA l)) s) of
--          ((Just f, s'), w) ->
--           case runWriter (runStateT (runMaybeT (runQueryA r)) s') of
--              ((Just a, s''), w') -> ((Just (f a), s''), w <> w')
--              ((Nothing, s''), w') -> ((Nothing, s''), w <> w')
--          ((Nothing, s'), w) ->
--           case runWriter (runStateT (runMaybeT (runQueryA r)) s') of
--              ((Just _, s''), w') -> ((Nothing, s''), w <> w')
--              ((Nothing, s''), w') -> ((Nothing, s''), w <> w')

-- instance Functor (QueryA r) where
--     fmap f (QueryA g) = QueryA $ fmap f g
-- instance ORM QueryA where
--   limit i = tellQuery (mkLimit "?" [ Q.One (Q.MySQLInt64 $ fromIntegral i)])
--   -- offset i = QueryA $ pure (mkOffset "OFFSET ?" [ Q.One (Q.MySQLInt64 $ fromIntegral i)])
--   col r c = tellQuery (mkSelect r c) *> empty
--   fromWith tab s = tellQuery (mkFrom ("`" <> unTable tab <> "` AS `" <> unARef s <> "`") [])
--   nestedWith = nestedListQuery
--   readonly a = coerce (() <$ a)
--   (=.) _ a = coerce a




-- newtype ParseResultsA r a = ParseResultsA { runParseResultsA :: State (SqlOut, [NestedResult]) a }
--   deriving (Functor, Applicative, Monad) via (State (SqlOut, [NestedResult]))
-- -- FIXME
-- popNextNested :: ParseResultsA r NestedResult
-- popNextNested = ParseResultsA $ do
--   (xs,ys) <- get
--   put (xs, tail ys)
--   pure $ head ys
-- type ParseResults = NameGen ParseResultsA

-- getBuilder :: QueryA a a -> QueryBuilder
-- getBuilder (QueryA q) = execWriter $ evalStateT (runMaybeT q) M.empty

-- instance ORM ParseResultsA where
--   limit _ = pure ()
--   col s t = ParseResultsA $ gets (\(a,_) -> getCol s t a)
--   fromWith _ _ = pure ()
--   nestedWith a r b = readListQuery a r b
--   readonly a = coerce (() <$ a)
--   (=.) _ a = coerce a

-- listOf :: Q.MySQLConn -> (forall m. ORM m => m a a) -> IO [a]
-- listOf conn s = do
--     let qb = getBuilder s
--     ls <- runQuery conn qb
--     nestedOutputs <- mapM (doNested conn (coerce ls)) (queryNested qb)
--     pure $ map (evalState (runParseResultsA s) . (,nestedOutputs)) (coerce ls)
-- -- runQuery :: Q.MySQLConn -> QueryBuilder -> IO [SqlOut]
-- -- runQuery conn qb = do
-- --     let q = formatQuery qb
-- --     print q
-- --     (_defs, stream) <- Q.query conn q (toList $ queryParams qb)
-- --     fmap (toSqlOut qb) <$> Streams.toList stream

-- doNested :: Q.MySQLConn -> [SqlOut] -> NestedCall -> IO NestedResult
-- doNested conn ls (NestedCall f) = do
--     o <- f conn ls
--     pure (NestedResult o)

-- type SqlFunctor a r = Q.MySQLConn -> (forall m. ORM m => m a a) -> IO  r

-- class LoadRelation r s | r -> s where
--     parentCol :: r -> [SomeSelect]
--     childCol :: r -> ARef s -> [SomeSelect]
--     toLookup :: r -> ARef s -> [SqlOut] -> SqlOut -> [SqlOut]
--     loadRelation :: Q.MySQLConn -> r -> ARef s -> QueryBuilder -> [SqlOut] -> IO [SqlOut]
--     relName :: r -> String

data SomeSelect = forall a b. SomeSelect (ARef a) (ColOf a b)
-- instance Show SomeSelect where
    -- show (SomeSelect r c) = dot r c
instance Eq SomeSelect where
    (SomeSelect (ARef r1) (ColOf c1 _)) == (SomeSelect (ARef r2) (ColOf c2 _)) = r1 == r2 && c1 == c2
instance Ord SomeSelect where
    compare (SomeSelect (ARef r1) (ColOf c1 _)) (SomeSelect (ARef r2) (ColOf c2 _)) = compare (r1, c1) (r2, c2)
-- data InnerJoin c s = forall p. InnerJoin { ijleft :: ARef p, ijright :: ATable s, ijleftCol :: ColOf p c, ijrightCol :: ColOf s c }
-- data ExternalJoin c s = forall p. ExternalJoin { ejleft :: ARef p, ejright :: ATable s, ejleftCol :: ColOf p c, ejrightCol :: ColOf s c }

-- jobUser :: ARef Job -> InnerJoin Int User
-- jobUser u = InnerJoin u users jobUserId userId
-- userJobs :: ARef User -> ExternalJoin Int Job
-- userJobs u = ExternalJoin u jobs userId jobUserId

-- instance (Ord c) => LoadRelation (InnerJoin c a) a where
--   parentCol InnerJoin {ijleft = parent, ijleftCol = lc} = [ SomeSelect parent lc ]
--   childCol InnerJoin {ijrightCol = rc} child = [ SomeSelect child rc ]

--   toLookup InnerJoin{..} ref rs = doLookup
--     where
--       doLookup row = case mapping M.!? lCol row of
--         Just o -> [o]
--         Nothing -> []
--       mapping = M.fromList [ (rCol r, r) | r <- rs]
--       rCol = getCol ref ijrightCol
--       lCol = getCol ijleft ijleftCol
--   loadRelation conn InnerJoin{..} ref qb parents = do
--       let parentCols = map (getColSql ijleft ijleftCol) parents
--       let source = mkFrom ("`" <> unTable ijright <> "` AS `" <> unARef ref <> "`") []
--       let inConstraint = whereIn ref ijrightCol parentCols
--       out <- runQuery conn (source <> inConstraint <> qb)
--       pure $ coerce out
--   relName InnerJoin {ijright = table} = unTable table
-- instance (Ord c) => LoadRelation (ExternalJoin c a) a where
--   parentCol ExternalJoin {ejleft = parent, ejleftCol = lc} = [ SomeSelect parent lc ]
--   childCol ExternalJoin {ejrightCol = rc} child = [ SomeSelect child rc ]

--   toLookup ExternalJoin{..} ref rs = doLookup
--     where
--       doLookup row = fromMaybe [] (mapping M.!? lCol row)
--       mapping = M.fromListWith (<>) [ (rCol r, [r]) | r <- rs]
--       rCol = getCol ref ejrightCol
--       lCol = getCol ejleft ejleftCol
--   loadRelation conn ExternalJoin{..} ref qb parents = do
--       let parentCols = map (getColSql ejleft ejleftCol) parents
--       let source = mkFrom ("`" <> unTable ejright <> "` AS `" <> unARef ref <> "`") []
--       let inConstraint = whereIn ref ejrightCol parentCols
--       out <- runQuery conn (source <> inConstraint <> qb)
--       pure $ coerce out
--   relName ExternalJoin {ejright = table} = unTable table

-- getMap :: (Ord k, Show k, Show v, HasCallStack) => M.Map k v -> k -> v
-- getMap m k = case m M.!? k of
--     Just v -> v
--     Nothing -> error $ "Missing key " <> show k <> " in " <> show m


-- toSqlOut :: QueryBuilder -> [Q.MySQLValue] -> SqlOut
-- toSqlOut qb ls = SqlOut m
--   where m = M.fromList $ zip (nubOrd $ querySelected qb) ls
-- getCol :: HasCallStack => ARef a -> ColOf a b -> SqlOut -> b
-- getCol rs c out = fromSql (colIso c) $ getColSql rs c out
-- getColSql :: HasCallStack => ARef a -> ColOf a b -> SqlOut -> Q.MySQLValue
-- getColSql rs c out = offsets out `getMap` SomeSelect rs c

-- data Job = Job
-- jobUserId :: ColOf Job Int
-- jobUserId = ColOf "user_id" sqlInt32U
-- jobId :: ColOf Job Int
-- jobId = ColOf "id" sqlInt32U
-- jobs :: ATable Job
-- jobs = ATable "jobs"

-- tellNestedCall :: NestedCall -> QueryA a a
-- tellNestedCall f =
--     tellQuery mempty{queryNested=[f]} *> empty

-- -- todo:
-- -- - move out name generation
-- -- - add diffing
-- -- - add topological sort
-- -- - add patch apply

-- readListQuery :: forall a r s. (LoadRelation r s, Typeable [a]) => r -> ARef s -> (forall m. ORM m => m a a) -> ParseResultsA [a] [a]
-- readListQuery _ _ _ = ParseResultsA $ do
--     (xs,_) <- get
--     unwrap xs <$> runParseResultsA popNextNested
--   where
--     unwrap out (NestedResult s) = case cast (s out) of
--        Just o -> o
--        Nothing -> error ("readListQuery: failed to cast: " <> show (typeOf s))
-- nestedListQuery :: forall a r s. (LoadRelation r s, Typeable [a]) => r -> ARef s -> (forall m. ORM m => m a a) -> QueryA [a] [a]
-- nestedListQuery rel ref inner =
--     ensureParentDependencyLoaded rel *>
--         let qb =  getBuilder (ensureChildDependencyLoaded rel ref *> inner)
--         in tellNestedCall (NestedCall @[a] (step qb))
--   where
--     step qb conn results = do
--       o <- loadRelation conn rel ref qb results
--       nestedOutputs <- mapM (doNested conn o) (queryNested qb)
--       let doLookup = toLookup rel ref o
--       let relationFor row = map (evalState (runParseResultsA inner) . (,nestedOutputs)) (doLookup row)
--       pure relationFor

-- newtype NameGen m r a = NameGen ( Compose (State (M.Map Char Int)) (m r) a )
--   deriving newtype (Applicative, Functor)

-- instance ORM m => ORM (NameGen m) where
--   limit = liftNameGen . limit
--   col a b = liftNameGen $ col a b
--   fromWith a b = liftNameGen $ fromWith a b
--   nestedWith a b c = liftNameGen $ nestedWith a b c
--   readonly a = mapNameGen readonly a
--   (=.) f a = mapNameGen (f =.) a

-- mapNameGen :: (m r a -> m s b) -> NameGen m r a -> NameGen m s b
-- mapNameGen f (NameGen a) = NameGen (Compose $ f <$> getCompose a)
-- flattenNamegen :: NameGen (NameGen m) r a -> NameGen m r a
-- flattenNamegen f = NameGen $ Compose (unNameGen =<< unNameGen f)
-- runNameGen :: NameGen m r a -> m r a
-- runNameGen (NameGen (Compose m)) = evalState m mempty
-- instance (forall x. Alternative (m x)) => Alternative (NameGen m r) where
--     empty = NameGen (Compose $ pure empty)
--     l <|> r = NameGen $ Compose $ liftA2 (<|>) (unNameGen l) (unNameGen r)
-- unNameGen :: NameGen m r a -> State (M.Map Char Int) (m r a)
-- unNameGen (NameGen (Compose m)) = m
-- class GenName m where
--     withName :: ATable s -> (ARef s -> m a) -> m a
-- liftNameGen :: m r a -> NameGen m r a
-- liftNameGen = NameGen . Compose . pure
-- instance GenName (NameGen m r) where
--     withName (ATable []) _ = undefined
--     withName (ATable (a:_)) k = NameGen $ Compose $ do
--       s <- gets (M.findWithDefault 0 a)
--       modify (M.insert a (s + 1))
--       case s of
--             0 -> unNameGen $ k $ ARef [a]
--             i -> unNameGen $ k $ ARef (a : show i)

-- from :: (GenName (m r), ORM m) => ATable s -> (ARef s -> m r a) -> m r a
-- from tab cont = withName tab $ \s -> fromWith tab s *> cont s
-- nested :: forall r s o m. (ORM m, LoadRelation r s, Typeable o) => r -> (forall n. ORM n => ARef s -> NameGen n o o) -> NameGen m [o] [o]
-- nested r cont = withName (ATable $ relName r) $ \s ->
--   let
--     cont' :: ORM n => NameGen n o o
--     cont' = cont s
--     nested' ::(forall n. ORM n => n o o) -> NameGen m [o] [o]
--     nested' a = nestedWith r s a
--   in nested' (runNameGen cont')

-- mainer :: IO ()
-- mainer = do
--   printResults $ from users $ \u ->  limit 10 *> ((,) <$> fst =. col u userFirstName <*> snd =. nested (userJobs u) (`col` jobId))


-- printResults :: Show a => (forall m. ORM m => NameGen m a a) -> IO ()
-- printResults m = do
--     conn <- testConnection
--     o <- listOf conn (runNameGen m)
--     print o
--     Q.close conn

-- newtype Unparse r a = Unparse { runUnParse :: (ReaderT r (Writer SqlOut)) a }
--   deriving newtype (Applicative, Functor, Monad, MonadReader r, MonadWriter SqlOut)
-- localReaderT :: (r' -> r) -> ReaderT r m a -> ReaderT r' m a
-- localReaderT f m = ReaderT $ \r -> runReaderT m (f r)
-- localUnparse :: (s -> r) -> Unparse r a -> Unparse s a
-- localUnparse f (Unparse m) = Unparse $ localReaderT f m
-- instance ORM Unparse where
--   limit _ = pure ()
--   col ref arg = do
--       r <- ask
--       let sql = toSql (colIso  arg) r
--       tell $ SqlOut (M.singleton (SomeSelect ref arg) sql)
--       pure r
--   fromWith _ _ = pure ()
--   nestedWith _ _ _ = ask
--   (=.) sr m = localUnparse sr m
--   readonly _ = pure ()
-- newtype Difference r a = Difference { runDifference :: (ReaderT (Delta r) (Writer (Diff SqlOut))) a }
--   deriving newtype (Applicative, Functor, Monad, MonadReader (Delta r), MonadWriter (Diff SqlOut))


-- unCont :: Unparse r r -> r -> SqlOut
-- unCont m r = execWriter $ runReaderT (runUnParse m) r

-- instance ORM Difference where
--   limit _ = pure ()
--   -- col ref arg = do
--   nestedWith rel ref cont = do
--     o <- ask
--     let old = fmap (unCont cont) <$> o
          
--     undefined




-- data Diff a = Diff
--     { delta :: Delta a
--     , children :: [ChildDiff a]
--     }
-- type ChildDiff a = [Diff a]
-- instance Semigroup a => Semigroup (Delta a) where
--     (Delete a) <> (Delete b) = Delete (a <> b)
--     (Insert a) <> (Insert b) = Insert (a <> b)
--     (Update a b) <> (Update c d) = Update (a <> c) (b <> d)
--     (Delete a) <> (Insert b) = Update a b
--     (Insert b) <> (Delete a) = Update a b
--     (Delete a) <> (Update b c) = Update (b <> a) c
--     (Update b c) <> (Delete a) = Update (b <> a) c
--     (Update _ b) <> (Insert c) = Insert (b <> c)
--     (Insert c) <> (Update _ b) = Insert (b <> c)
--     (Noop _) <> a = a
--     a <> (Noop _) = a
-- instance Monoid a => Monoid (Delta a) where
--     mempty = Noop mempty
-- instance Semigroup a => Semigroup (Diff a) where
--     Diff d1 c1 <> Diff d2 c2 = Diff (d1 <> d2) (c1 <> c2)
-- instance Monoid a => Monoid (Diff a) where
--     mempty = Diff mempty mempty
