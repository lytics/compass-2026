import { LivePreviewTypeMapper, PageEntry } from '@/types/common'
import { Article, pageRenderProps } from './common'

export type Homepage = {
    entry: PageEntry & pageRenderProps
}

export type LandingPage = {
    entry: PageEntry & pageRenderProps 
}

export type ArticlePage = {
  entry: PageEntry & Article
  articles?: Article[] | []
  $?: LivePreviewTypeMapper<ArticlePage>
}

export type ArticleListingPage = {
  articles?: Article[] | []
  entry?:PageEntry & pageRenderProps
}