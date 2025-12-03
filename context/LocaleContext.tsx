'use client'
import { createContext, useContext } from 'react'

interface LocaleContextType {
    currentLocale: string
}
    
// use LocaleContext to get currentLocale and CMS locale collection
export const LocaleContext = createContext<LocaleContextType>({} as LocaleContextType)
  
export const useLocaleContext = () => useContext(LocaleContext)