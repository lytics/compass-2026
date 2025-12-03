export function classNames (...classes: string[]) {
    return classes.filter(Boolean).join(' ')
}

export function statusValues (...val: boolean[]) {
    return val.every((x) => x)
}

export const equalHeight = (selector:string) => {
    let newHeight = 0
    const tilesHead:any = document.querySelectorAll(selector)
    tilesHead?.length > 0 && tilesHead?.forEach((t:any) => {
        t.style.height = 'inherit'
        if (t.getBoundingClientRect().height > newHeight) newHeight = t.getBoundingClientRect().height
    })

    // for tablet and mobile
    if (window.innerWidth > 430) {
        tilesHead?.length > 0 && tilesHead?.forEach((t:any) => {
            t.style.height = `${newHeight}px`
        })
    } 
}