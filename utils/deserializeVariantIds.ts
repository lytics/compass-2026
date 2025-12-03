import { Sdk } from '@contentstack/personalize-edge-sdk/dist/sdk'

export function deserializeVariantIds (personalizationSDK : Sdk | undefined) : string {
    try {
        if(!personalizationSDK) return ''
        return personalizationSDK.getVariantAliases().join(',')
    } catch (err) {
        console.error('Error while deserializing variant ids : ', err)
        return ''
    }
}