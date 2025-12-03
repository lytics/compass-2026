'use client'

import Slider from 'react-slick'
import {ChevronLeftIcon, ChevronRightIcon} from '@heroicons/react/24/outline'
import { Carousel as CarouselType } from '@/types/components'
import './carousel.css'

const SlickNavigationButton = (
    props: {
      children: JSX.Element;
      slideCount?: number;
      currentSlide?: number;
    }
) => {
    const { children, ...others } = props
    delete others.slideCount
    delete others.currentSlide
    return (
        <span {...others} >
            {children}
        </span>
    )
}

const Carousel = ({className, children}: CarouselType) => {

    const settings = {
        dots: false,
        infinite: false,
        speed: 500,
        slidesToShow: 6,
        slidesToScroll: 6,
        nextArrow: <SlickNavigationButton><ChevronRightIcon className='h-5 my-auto'/></SlickNavigationButton>,
        prevArrow: <SlickNavigationButton><ChevronLeftIcon className='h-5 my-auto'/></SlickNavigationButton>,
        responsive: [
            {
                breakpoint: 1280,
                settings: {
                    infinite: false,
                    slidesToShow: 3,
                    slidesToScroll: 3,
                    nextArrow: <SlickNavigationButton><ChevronRightIcon className='h-5 my-auto !text-black hover:!text-gray-900'/></SlickNavigationButton>,
                    prevArrow: <SlickNavigationButton><ChevronLeftIcon className='h-5 my-auto !text-gray-600 hover:!text-gray-900'/></SlickNavigationButton>
                    
                }
            }
        ]
    }

    return (
        <Slider {...settings} className={className}>
            {children}
        </Slider>
    )
}

export {Carousel}