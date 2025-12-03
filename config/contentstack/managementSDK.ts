
// This file is used to create the headers for the management SDK which includes the contentstack api_key, authorization and branch headers needed to make the management API calls
export const createManagmentHeaders = (method: string) => {
    if (!process.env?.CONTENTSTACK_API_KEY && !process.env.CONTENTSTACK_MANAGEMENT_TOKEN) { // validates whether the keys are present in .env.local file
        throw 'management header keys not found, please include CONTENTSTACK_API_KEY, CONTENTSTACK_MANAGEMENT_TOKEN in .env.local file'
    }
    const myHeaders = new Headers() // create a new Headers object
    process.env.CONTENTSTACK_API_KEY && myHeaders.append('api_key', process.env.CONTENTSTACK_API_KEY) // append the contentstack api_key to the headers
    process.env.CONTENTSTACK_MANAGEMENT_TOKEN && myHeaders.append('authorization', process.env.CONTENTSTACK_MANAGEMENT_TOKEN) // append the contentstack authorization token to the headers
    process.env.CONTENTSTACK_BRANCH && myHeaders.append('branch', process.env.CONTENTSTACK_BRANCH) // append the contentstack branch to the headers

    // return the headers object with method
    return {
        method: method,
        headers: myHeaders
    }
}