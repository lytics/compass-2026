'use client'
import React, { useEffect, useState } from 'react'
import { SEO } from '@/components'
import { SeoProps } from '@/types/pages'
import { getJsonCookie } from '@/utils'
import { localeCookieName } from '@/config'

/**
 * A wrapper component that provides SEO functionality for pages
 * @component
 * @param {Object} props - Component props
 * @param {string} props.locale - Current locale/language code
 * @param {string} props.title - Page title
 * @param {string} props.summary - Page description/summary
 * @param {string} props.url - Page URL
 * @param {Object} props.seo - Additional SEO metadata
 * @param {React.ReactNode} props.children - Child components to render
 * @returns {JSX.Element} Wrapped page with SEO metadata
 */

const PageWrapper:React.FC<SeoProps & React.PropsWithChildren> = ({ locale, title, summary, url, seo, children}: SeoProps & React.PropsWithChildren) => {
    const [locales, setLocales] = useState<SeoProps['locales']>([])

    useEffect(() => {
        const localesArray = getJsonCookie(localeCookieName)
        localesArray?.length > 0 && setLocales(localesArray)
    },[])

    return <>
        <SEO
            url={url}
            locale={locale}
            title={title}
            seo={seo}
            summary={summary}
            locales={locales}
        />
        {children}
    </>
}

export { PageWrapper }