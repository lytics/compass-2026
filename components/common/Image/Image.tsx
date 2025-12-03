import { ImageComponent } from '@/types/components'
import { ImagePlaceholder } from '@/components'

/**
 * Image component that renders a responsive picture element with multiple source elements for different screen sizes
 * or a placeholder if no image URL is provided.
 * 
 * @param {Object} props - Component props
 * @param {Object} props.image - Image object containing URL and title
 * @param {string} props.image_alt_text - Alternative text for the image
 * @param {string} props.className - CSS class names for styling
 * @param {boolean} props.is_thumbnail - Flag to determine if image should be rendered as thumbnail
 * @param {boolean} props.addDataCslp - Flag to add data attributes from image object
 * @param {Object} props.$ - Optional object containing data-cslp attributes
 * @returns {JSX.Element} Picture element with responsive sources or placeholder
 */

const Image: React.FC<ImageComponent> = (props: ImageComponent) => {
    const { image, image_alt_text, className, is_thumbnail, addDataCslp=true, $ } = props

    const queryParam = (is_thumbnail && is_thumbnail !== undefined ) ?  'auto=webp&format=pjpg&width=50p' : 'auto=webp&format=pjpg'

    return <>
        {image?.url ? <picture
        >
            <source media='(max-width: 475px)' srcSet={image?.url.indexOf('?') > -1 ? `${image.url}&${queryParam}` : `${image.url}?${queryParam}`} />
            <source media='(min-width: 476px) and (max-width: 640px)' srcSet={image?.url.indexOf('?') > -1 ? `${image.url}&${queryParam}` : `${image.url}?${queryParam}`} /> {/* xs */}
            <source media='(min-width: 641px) and (max-width: 767px)' srcSet={image?.url.indexOf('?') > -1 ? `${image.url}&${queryParam}` : `${image.url}?${queryParam}`} /> {/* sm */}
            <source media='(min-width: 768px) and (max-width: 1024px)' srcSet={image?.url.indexOf('?') > -1 ? `${image.url}&${queryParam}` : `${image.url}?${queryParam}`} /> {/* md */}
            <source media='(min-width: 1024px) and (max-width: 1279px)' srcSet={image?.url.indexOf('?') > -1 ? `${image.url}&${queryParam}` : `${image.url}?${queryParam}`} /> {/* lg */}
            <source media='(min-width: 1280px) and (max-width: 1535px)' srcSet={image?.url.indexOf('?') > -1 ? `${image.url}&${queryParam}` : `${image.url}?${queryParam}`} />{/* xl */}
            <source media='(min-width: 1536px)' srcSet={image?.url.indexOf('?') > -1 ? `${image.url}&${queryParam}` : `${image.url}?${queryParam}`} />{/* 2xl */}
            <img
                src={image?.url.indexOf('?') > -1 ? `${image.url}&${queryParam}` : `${image.url}?${queryParam}`}
                alt={(image_alt_text !== undefined && image_alt_text !== '') ? image_alt_text : image?.title}
                className={className}
                data-id='image-component'
                {...(addDataCslp && image?.$?.url)}
            />
        </picture>
            : <ImagePlaceholder className={className || ''} $={($?.image || $?.cover_image)} />}
    </>
}
export { Image }