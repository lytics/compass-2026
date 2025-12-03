export const setDataForChromeExtension = (data: { entryUid: string, contenttype: string, locale: string }) => {
    document.body.setAttribute('data-pageref', data.entryUid)
    document.body.setAttribute('data-contenttype', data.contenttype)
    document.body.setAttribute('data-locale', data.locale)
}