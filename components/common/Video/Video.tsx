import { VideoComponent } from '@/types/components'

/**
 * Video component that renders a video element with specified properties
 * @component
 * @param {Object} props - Component props
 * @param {Object} props.video - Video object containing URL and other properties
 * @param {string} props.video_alt_text - Alternative text for the video
 * @param {string} props.className - CSS class name for styling
 * @param {boolean} [props.addDataCslp=true] - Flag to add data CSLP attributes
 * @returns {JSX.Element} Video component
 */

const Video: React.FC<VideoComponent> = (props: VideoComponent) => {
    const { video, video_alt_text, className, addDataCslp=true } = props

    return <>
        {video?.url && (
            <video
                src={video?.url}
                title= {video_alt_text || 'Alt Text for Video in Teaser'}
                {...(addDataCslp && video?.$?.url)}
                className={className}
                data-id='video-component'
                muted autoPlay loop
                {...(addDataCslp && video?.$?.url)}
            />
        )}
    </>
}

export { Video }