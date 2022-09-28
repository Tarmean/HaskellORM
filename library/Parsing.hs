{-# Language OverloadedStrings #-}
{-# Language FlexibleContexts #-}
{-# Language NamedFieldPuns #-}
module Parsing where

import Text.Megaparsec
import qualified Text.Megaparsec.Char.Lexer as Lexer
import Debug.Trace
import Text.Megaparsec.Char (string)
import qualified Data.Text as T
import Control.Monad.State
import Data.Void
import Data.Char
import qualified Data.Set as E
import Data.Function (on)

type Parser = Parsec Void T.Text

data PosKind = PosKind { pos :: SourcePos, isHead :: Bool }
  deriving (Eq, Ord, Show)
data S = S { blockOffset :: Maybe PosKind }
  deriving (Eq, Ord, Show)
type M = StateT S Parser

block :: (TraversableStream f, MonadParsec e f m, MonadState S m) => m a -> m a
block m = do
    old <- gets blockOffset
    new <- getSourcePos
    modify $ \s -> s { blockOffset = Just $ PosKind new True }
    out <- m
    modify $ \s -> s { blockOffset = old }
    pure out

guardPos :: (TraversableStream f, MonadFail m, MonadParsec e f m, MonadState S m) => (Bool -> SourcePos -> SourcePos -> Bool) -> m ()
guardPos f = do
    old <- gets blockOffset
    new <- getSourcePos
    case old of
        Nothing -> fail "elemGE outside block"
        Just PosKind {pos, isHead}
          | f isHead new pos -> modify $ \s -> s { blockOffset = Just $ PosKind new False }
          | otherwise -> fancyFailure . E.singleton $ ErrorIndentation GT (sourceColumn pos) (sourceColumn new)

indentGE :: M ()
indentGE = guardPos predicate
  where
    predicate isHead a b
      | sourceLine a <= sourceLine b = False
      | isHead && sourceColumn a == sourceColumn b = False
      | sourceColumn a < sourceColumn b = False
      | otherwise = True




lexeme :: M a -> M a
lexeme p = p <* takeWhileP (Just "whitespace") isSpace

lexeme1 :: M a -> M a
lexeme1 p = p <* takeWhile1P (Just "whitespace") isSpace


blockElem :: M a -> M a
blockElem m = do
    indentGE
    lexeme m


parseHead :: M (T.Text, T.Text)
parseHead = do
    lexeme1 $ string "for"
    var <- lexeme1 $ takeWhile1P (Just "AlphaNum") isAlphaNum
    lexeme1 $ string "in"
    expr <- lexeme $ takeWhile1P (Just "expr") $ (/= ':')
    lexeme $ string ":"
    pure (var, expr)

body :: M T.Text
body = lexeme $ takeWhile1P (Just "AlphaNum")  isAlphaNum

listP :: M (T.Text, T.Text, [T.Text])
listP =
    block $ do
        (l,r) <- parseHead
        tail <- some (indentGE *> body)
        pure (l,r,tail)

testParse :: M a -> T.Text -> a
testParse p t = case runParser (evalStateT (p <* eof) emptyPos) "" t of
    Left e -> error $ errorBundlePretty e
    Right a -> a
  where emptyPos = S { blockOffset = Nothing }
