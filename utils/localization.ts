
export const isLocale  = (slug: string)  => {
    // Regular expression to match valid locale codes (e.g., 'en', 'en-us', 'zh-cht', 'lt-az-az')
    const localeRegex = /^[a-z]{2,3}(-[a-z]{2,3}){0,2}$|^[a-z]{2,3}-[a-z]{2}-[a-z]{2}$/
    return localeRegex.test(slug)
}

export const getUnlocalizedPath = (pathname: string) => {
    return '/' + pathname.split('/').filter((x) => !isLocale(x)).join('/')
}

export const getFlagCode = (code?: string) : string => {
    try {
        if (!code) return ''
        if (code === 'en') return 'us'
        if (code?.length > 4) {
            const hyphenatedCode = code?.split('-')
            const hyphenatedFlagCode = hyphenatedCode ? hyphenatedCode[hyphenatedCode.length - 1] : ''
            return getFlagCode(hyphenatedFlagCode?.substring(0,2))
        }
        return code ? new Intl.Locale(code)?.region?.toLowerCase() || new Intl.Locale(code)?.language?.toLowerCase() : ''
    } catch (err) {
        console.error('🚀 ~ getFlagCode ~ err:', err)
        return ''
    }
}