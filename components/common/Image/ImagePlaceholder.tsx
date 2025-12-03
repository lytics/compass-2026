/* eslint-disable max-len */

/**
 * ImagePlaceholder component displays a placeholder for images during live edit mode
 * @component
 * @param {Object} props - Component props
 * @param {string} props.className - Additional CSS classes to apply to the placeholder
 * @param {Object} props.$ - Optional object containing data-cslp attributes
 * @returns {JSX.Element|null} Returns placeholder div in live edit mode, null otherwise
 */
import { isDataInLiveEdit } from '@/utils'

const ImagePlaceholder: React.FC<{className?: string, $?: {'data-cslp'?: string}}>  
        = (props: {className?: string, $?: {'data-cslp'?: string}}) => {
            return isDataInLiveEdit() && <div
                className={`${props?.className} !bg-gray-500 flex items-center justify-center`}
                data-id='image-placeholder-component'
                {...props?.$}
            >
                <svg className='w-10 h-10 text-gray-200 ' aria-hidden='true' xmlns='http://www.w3.org/2000/svg' fill='currentColor' viewBox='0 0 20 18'>
                    <path d='M18 0H2a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V2a2 2 0 0 0-2-2Zm-5.5 4a1.5 1.5 0 1 1 0 3 1.5 1.5 0 0 1 0-3Zm4.376 10.481A1 1 0 0 1 16 15H4a1 1 0 0 1-.895-1.447l3.5-7A1 1 0 0 1 7.468 6a.965.965 0 0 1 .9.5l2.775 4.757 1.546-1.887a1 1 0 0 1 1.618.1l2.541 4a1 1 0 0 1 .028 1.011Z' />
                </svg>
            </div>
        }

export {ImagePlaceholder}