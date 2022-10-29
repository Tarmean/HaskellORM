{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
module FreeAp where



data Ap f r a where
  Pure :: a -> Ap f r a
  LiftF :: f r a -> Ap f r a
  Ap :: Ap f r (a -> b) -> Ap f r a -> Ap f r b
  LMap :: (r -> r') -> Ap f r' a -> Ap f r a

instance Functor (Ap f ra) where
    fmap f a = Ap (Pure f) a

instance Applicative (Ap f r) where
    pure = Pure
    (<*>) = Ap
runAp :: Applicative g => (forall r' x. f r' x -> g x) -> Ap f r a -> g a
runAp alg (LiftF f) = alg f
runAp _alg (Pure a) = pure a
runAp alg (LMap _ a) = runAp alg a
runAp alg (Ap f g) = runAp alg f <*> runAp alg g


runAp_ :: Monoid m => (forall r' a. f r' a -> m) -> Ap f r b -> m
runAp_ alg (LiftF f) = alg f
runAp_ _alg (Pure _) = mempty
runAp_ alg (LMap _ a) = runAp_ alg a
runAp_ alg (Ap f g) = runAp_ alg f <> runAp_ alg g

runBAp :: forall f g a n. Applicative g => (forall r' x. r' -> f r' x -> g x) -> Ap f n a -> n -> g a
runBAp alg a0 = go a0
  where
    go :: Ap f r b -> r -> g b
    go (Pure a) _x = pure a
    go (LiftF f) x = alg x f
    go (Ap f g) x = go f x <*> go g x
    go (LMap f a) x = go a (f x)

runBAp_ :: forall f r g a. Monoid g => (forall r' x. r' -> f r' x -> g) -> Ap f r a -> r -> g
runBAp_ alg a0 = go a0
  where
    go :: Ap f r' b -> r' -> g
    go (Pure _) _x = mempty
    go (LiftF f) x = alg x f
    go (Ap f g) x = go f x <> go g x
    go (LMap f a) x = go a (f x)

(=.) :: (r -> r') -> Ap f r' a -> Ap f r a
(=.) = LMap

liftAp :: f r a -> Ap f r a
liftAp = LiftF
