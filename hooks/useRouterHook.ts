'use client'
import { useParams, usePathname, useRouter, useSearchParams } from 'next/navigation'
import { useEffect, useState } from 'react'
import { getJsonCookie } from '@/utils'
import { localeCookieName } from '@/config'

const useRouterHook = () => {
    const [localesCodeArray, setlocalesCodeArray] = useState<string[]>([])
    const Router = useRouter()
    const Pathname = usePathname()
    const Params = useParams()
    const SearchParams = useSearchParams()

    useEffect(() => {
        const locales = getJsonCookie(localeCookieName)
        locales?.length > 0 && setlocalesCodeArray(locales.map((loc: { code: string }) => loc.code))
    },[])
    
    const getLocale = () => { // returns current locale from params
        return Params?.locale as string
    }

    const getUnlocalizedPath = () => {
        const pathArray = Pathname.split('/')
        // home page path
        if ( pathArray?.length === 2 && pathArray?.[0] === '' && localesCodeArray?.includes(pathArray?.[1]) ) {
            return '/'
        }
        //  other pages
        if (Pathname?.length && localesCodeArray?.length > 0) {
            return Pathname.split('/').filter(slug => !localesCodeArray?.includes(slug)).join('/')
        } else {
            return pathArray?.length > 2 ? '/' + pathArray.slice(2).join('/') : '/'
        }
    }

    const getLocalizedPath = () => {
        return Pathname
    }

    const getStringfiedSearchParams = () => {
        return SearchParams?.toString()?.length ? `?${SearchParams.toString()}` : ''
    }

    return {
        path: getUnlocalizedPath(),
        locale: getLocale(),
        localizedPath: getLocalizedPath(),
        searchParams: SearchParams,
        stringfiedSearchParams: getStringfiedSearchParams(),
        ...Router

    }
}

export default useRouterHook